#!/usr/bin/env python3
"""Frozen-snapshot, read-only FotMob completion audit (stdlib client).

The scheduler supplies trino and credentials. This module has no project imports.
All content artifacts exclude clocks, query IDs and invocation labels.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


SQL_DIR = Path(__file__).with_name("fotmob_completion_audit")
SECTIONS = ("matchFacts", "stats", "playerStats", "lineup", "shotmap", "momentum")
PRIORITY = ("unknown", "writer_failed", "parse_failed", "fetch_failed",
            "source_unavailable", "not_applicable", "confirmed_content")
SUCCESS = {"success", "not_modified"}
LIMITS = {"scopes": 25, "ids": 5000, "sql_bytes": 128 * 1024,
          "statement_seconds": 25, "pass_seconds": 10800, "bucket_count": 16}
CONTENT_FILES = ("current-id-evidence.csv", "mismatched-ids.csv",
                 "section-evidence.csv", "section-mismatches.csv", "marker-results.csv", "scope-summary.csv",
                 "historical-1294-results.csv", "c1-reconciliation.json",
                 "candidate-anomalies.json")
LEDGER_FIELDS = ("step", "chunk", "bucket", "query_id", "sql_sha256", "rows",
                 "wall_seconds", "status", "error_class")
ID_FIELDS = ("competition_id", "source_season_key", "match_id", "scoped_id",
             "finished", "cancelled", "postponed", "awarded", "future",
             "calendar_batch", "calendar_observed_at", "payload_present",
             "payload_batch", "payload_observed_at", "required", "marker_only",
             "historical_1294", "category", "reason", "late_failure",
             "evidence_ids", "evidence_sha256")
MISMATCH_FIELDS = ID_FIELDS
SECTION_FIELDS = ("competition_id", "source_season_key", "match_id", "section",
                  "available", "typed", "raw", "category", "reason", "payload_batch")
SUMMARY_FIELDS = ("competition_id", "source_season_key", "catalog_name", "required",
                  "marker_only", "historical_1294", "calendar_state", "known",
                  "finished", "with_payload", "without_payload", "actionable_gap",
                  "payload_rows", "known_id_set_hash", "payload_id_set_hash",
                  "mismatch_id_set_hash", "reason_counts", "completion_reason_counts", "section_reason_counts")
MARKER_FIELDS = ("competition_id", "source_season_key", "plan_signature", "entity_id", "cohort",
                 "target_key", "batch_id", "status", "category", "reason_counts",
                 "scope_keys", "evidence_sha256")


class AuditError(RuntimeError):
    """An explicit, fail-closed audit error."""


class QueryError(AuditError):
    def __init__(self, error_class, query_id=""):
        self.error_class = error_class
        self.query_id = query_id
        super().__init__(f"query failed: {error_class}; query_id={query_id or 'unavailable'}")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    indent=2) + "\n", encoding="utf-8")


def load(path):
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def write_csv(path, fields, rows):
    with Path(path).open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n",
                                extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: canonical(value) if isinstance(value, (dict, list))
                             else (int(value) if isinstance(value, bool) else value)
                             for key, value in row.items()})


def read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def integer(value, positive=True):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
        raise AuditError("expected an unsigned integer")
    number = int(value)
    if positive and number <= 0:
        raise AuditError("expected a positive integer")
    return number


def quote_literal(value):
    if not isinstance(value, str) or "\x00" in value:
        raise AuditError("invalid SQL literal")
    return "'" + value.replace("'", "''") + "'"


def scope_key(row):
    return integer(row["competition_id"]), str(row["source_season_key"])


def scoped_id(row):
    return canonical([integer(row["competition_id"]), row["source_season_key"],
                      integer(row["match_id"])])


def id_set_hash(ids):
    return sha("\0".join(sorted(set(ids))).encode("utf-8"))


def chunks(values, size):
    if size <= 0:
        raise AuditError("chunk size must be positive")
    return [values[i:i + size] for i in range(0, len(values), size)]


def scope_predicate(scopes):
    keys = sorted({scope_key(row) for row in scopes})
    if not keys:
        return "FALSE"
    return "(" + " OR ".join(
        f"(competition_id = {cid} AND source_season_key = {quote_literal(season)})"
        for cid, season in keys) + ")"


def manifest_scope_predicate(scopes):
    keys = sorted({scope_key(row) for row in scopes})
    return "(" + " OR ".join(
        f"(competition_id = {quote_literal(str(cid))} AND source_season_key = {quote_literal(season)})"
        for cid, season in keys) + ")" if keys else "FALSE"


def in_predicate(column, values, numeric=False):
    if column not in {"match_id", "entity_id", "_target_batch_id", "batch_id"}:
        raise AuditError("unapproved predicate column")
    vals = sorted(set(integer(v) if numeric else str(v) for v in values))
    if not vals:
        return "FALSE"
    encoded = [str(v) if numeric else quote_literal(v) for v in vals]
    return column + " IN (" + ",".join(encoded) + ")"


def guard_sql(sql, maximum=LIMITS["sql_bytes"]):
    if len(sql.encode("utf-8")) > maximum:
        raise QueryError("SQLTooLarge")
    # Strip quoted strings/identifiers before looking for statement delimiters.
    # Comments are forbidden, so they cannot conceal statements or mutations.
    tokenized = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", " quoted ", sql)
    if "--" in tokenized or "/*" in tokenized or ";" in tokenized:
        raise AuditError("comments/multiple statements are forbidden")
    words = re.findall(r"[A-Za-z_]+", tokenized.lower())
    if not words or words[0] not in {"select", "with", "show", "describe", "explain"}:
        raise AuditError("only a read-only statement is permitted")
    forbidden = {"insert", "update", "delete", "merge", "create", "drop", "alter",
                 "truncate", "call", "grant", "revoke", "analyze", "refresh",
                 "execute", "prepare", "deallocate", "commit", "rollback", "set",
                 "reset", "use", "vacuum"}
    if forbidden.intersection(words):
        raise AuditError("write/administrative SQL is forbidden")
    if re.search(r"\b\w+_current\b", sql, flags=re.I):
        raise AuditError("current views are forbidden")
    # Data-table scans must be pinned; history scans are the sole planning exception.
    for match in re.finditer(r"\b(?:from|join)\s+((?:[\w]+\.)+[\w\"]+)", sql, re.I):
        table = match.group(1)
        if "$history" not in sql[match.start():match.end() + 12]:
            tail = sql[match.end():]
            if not re.match(r"\s+FOR\s+VERSION\s+AS\s+OF\s+[1-9][0-9]*\b", tail, re.I):
                raise AuditError(f"unversioned base table: {table}")
    return sql


def render(name, snapshots=None, **params):
    values = {}
    for key, value in (snapshots or {}).items():
        values[key + "_snapshot"] = integer(value["snapshot_id"])
    for key, value in params.items():
        values[key] = integer(value, positive=(key != "bucket")) if key in {
            "bucket", "bucket_count"} else value
    return (SQL_DIR / (name + ".sql")).read_text(encoding="utf-8").format(**values)


REMOTE_SCRIPT = r'''
import os, sys, json, trino
connection = cursor = None
result = None
try:
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8443")), user="src_fotmob",
        http_scheme="https", auth=trino.auth.BasicAuthentication(
            "src_fotmob", os.environ["TRINO_SRC_FOTMOB_PASSWORD"]),
        verify=False, source="fotmob:2256:" + sys.argv[2],
        request_timeout=(3, 30), max_attempts=1,
        session_properties={"enable_dynamic_filtering": "false",
                            "query_max_run_time": "25s"})
    cursor = connection.cursor()
    cursor.execute(sys.argv[1])
    rows = cursor.fetchall()
    result = {"ok": True, "query_id": cursor.query_id,
              "columns": [column[0] for column in cursor.description], "rows": rows}
except Exception as exc:
    error_class = ("ServerTimeout" if getattr(exc, "error_name", "") in
                   {"EXCEEDED_TIME_LIMIT", "EXCEEDED_CPU_LIMIT"} else type(exc).__name__)
    result = {"ok": False, "error_class": error_class,
              "query_id": getattr(cursor, "query_id", None)}
finally:
    try:
        if cursor is not None:
            cursor.close()
    except Exception:
        pass
    try:
        if connection is not None:
            connection.close()
    except Exception:
        pass
print(json.dumps(result, default=str))
'''


class RemoteTrino:
    def __init__(self, run):
        self.run = validate_name(run)

    def execute(self, sql):
        guard_sql(sql)
        try:
            process = subprocess.run(
                ["docker", "exec", "--workdir", "/tmp", "airflow-scheduler", "python",
                 "-B", "-W", "ignore", "-c", REMOTE_SCRIPT, sql, self.run],
                capture_output=True, text=True, timeout=35, check=False)
        except subprocess.TimeoutExpired as exc:
            raise QueryError("ClientTimeout") from exc
        if process.returncode:
            raise QueryError("RemoteProcessError")
        try:
            result = json.loads(process.stdout)
            if not result["ok"]:
                error = re.sub(r"[^A-Za-z0-9_]", "", result["error_class"])[:100]
                raise QueryError(error, result.get("query_id") or "")
            rows = [dict(zip(result["columns"], row, strict=True)) for row in result["rows"]]
            return {"query_id": result["query_id"], "rows": rows}
        except (ValueError, KeyError, TypeError) as exc:
            raise QueryError("InvalidRemoteEnvelope") from exc


class Queries:
    """Sequential execution with a durable ledger, including failed attempts."""
    def __init__(self, transport, ledger, budget=3600):
        self.transport = transport
        self.path = Path(ledger)
        self.rows = []
        self.started = time.monotonic()
        self.budget = budget
        write_csv(self.path, LEDGER_FIELDS, [])

    def run(self, sql, step, chunk="", bucket=""):
        entry = dict(zip(LEDGER_FIELDS, [step, chunk, bucket, "", sha(sql.encode()),
                                        0, 0, "failed", ""]))
        started = time.monotonic()
        try:
            if started - self.started >= self.budget:
                raise QueryError("PassDeadline")
            guard_sql(sql)
            result = self.transport.execute(sql)
            entry["query_id"] = result.get("query_id") or ""
            if not entry["query_id"]:
                raise QueryError("MissingQueryId")
            entry["rows"] = len(result["rows"])
            elapsed = time.monotonic() - started
            if elapsed > LIMITS["statement_seconds"]:
                raise QueryError("StatementBudgetExceeded", entry["query_id"])
            if time.monotonic() - self.started > self.budget:
                raise QueryError("PassDeadline", entry["query_id"])
            entry["status"] = "success"
            return result["rows"]
        except Exception as exc:
            entry["error_class"] = getattr(exc, "error_class", type(exc).__name__)
            entry["query_id"] = getattr(exc, "query_id", entry["query_id"])
            raise
        finally:
            entry["wall_seconds"] = round(time.monotonic() - started, 6)
            self.rows.append(entry)
            write_csv(self.path, LEDGER_FIELDS, self.rows)


def validate_name(name):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", name):
        raise AuditError("invalid run/pass name (letters, digits, underscore, dash only)")
    return name


def json_object(value):
    if isinstance(value, dict):
        return value, True
    if value in (None, ""):
        return {}, True
    try:
        decoded = json.loads(value)
        return (decoded, True) if isinstance(decoded, dict) else ({}, False)
    except (TypeError, ValueError):
        return {}, False


def timestamp(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace(" UTC", "+00:00").replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (ValueError, OverflowError):
        return None


def is_v2(row):
    return bool(re.search(r"(^|[-_:])v?2$", str(row.get("parser_version", ""))))


def eligible(row):
    v1 = bool(re.search(r"(^|[-_:])v?1$", str(row.get("parser_version", ""))))
    return ((is_v2(row) and row.get("status") in SUCCESS | {"not_available"}) or
            (v1 and row.get("status") in SUCCESS))


def manifest_rank(row):
    capabilities, _ = json_object(row.get("capabilities_json"))
    observed = timestamp(capabilities.get("source_observed_at"))
    if observed is None:
        observed = timestamp(row.get("fetched_at"))
    if observed is None:
        observed = timestamp(row.get("completed_at"))
    return (int(is_v2(row)), observed if observed is not None else float("-inf"),
            timestamp(row.get("completed_at")) or float("-inf"), str(row.get("batch_id", "")))


def chronology(row):
    return (timestamp(row.get("completed_at")) or timestamp(row.get("fetched_at")) or
            float("-inf"), str(row.get("batch_id", "")))


def physical_rank(row):
    return (int(is_v2(row)), timestamp(row.get("_observed_at")) or float("-inf"),
            timestamp(row.get("_ingested_at")) or float("-inf"),
            str(row.get("_target_batch_id", "")))


def deduplicate(rows):
    result = {}
    for row in rows:
        key = (*scope_key(row), integer(row["match_id"]))
        previous = result.get(key)
        if previous is None or physical_rank(row) > physical_rank(previous):
            result[key] = row
        elif physical_rank(row) == physical_rank(previous) and canonical(row) != canonical(previous):
            raise AuditError("ambiguous physical duplicate with identical ordering keys")
    return result


def choose_calendar(candidates, presence):
    """Presence keys include scope: an unrelated scope cannot qualify a batch."""
    grouped = defaultdict(list)
    for row in candidates:
        if eligible(row):
            grouped[scope_key(row)].append(row)
    winners, anomalies = {}, []
    for key, rows in sorted(grouped.items()):
        ordered = sorted(rows, key=manifest_rank, reverse=True)
        for row in ordered:
            _, valid = json_object(row.get("capabilities_json"))
            if not valid:
                anomalies.append({"scope": list(key), "batch_id": row["batch_id"],
                                  "reason": "malformed_candidate_capabilities"})
        for row in ordered:
            if row["status"] == "not_available" or (*key, str(row["batch_id"])) in presence:
                winners[key] = row
                break
            anomalies.append({"scope": list(key), "batch_id": row["batch_id"],
                              "reason": "empty_calendar_success_skipped"})
    return winners, anomalies


def choose_global(rows):
    grouped = defaultdict(list)
    for row in rows:
        if eligible(row):
            grouped[integer(row["entity_id"])].append(row)
    return {key: max(values, key=manifest_rank) for key, values in grouped.items()}


def current_payloads(rows, calendar, winners):
    accepted = []
    for row in rows:
        key = (*scope_key(row), integer(row["match_id"]))
        winner = winners.get(key[2])
        if winner and winner["status"] in SUCCESS and str(row["_target_batch_id"]) == str(winner["batch_id"]):
            winner["physical_batch_present"] = True
        if key in calendar and winner and winner["status"] in SUCCESS and str(
                row["_target_batch_id"]) == str(winner["batch_id"]):
            accepted.append(row)
    return deduplicate(accepted)


def flag(value):
    return value is True or value == 1 or str(value).lower() in {"true", "1"}


def nonempty(value):
    # bool/0/empty strings are content under the service contract.
    return value is not None and value != {} and value != []


def parse_failure(row):
    code = str(row.get("error_code") or "").lower()
    return (row.get("status") == "schema_drift" or row.get("fetch_outcome") == "schema_drift"
            or code == "schema_drift" or code.startswith(("parse_", "parser_", "schema_")))


def applicability(match):
    for name in ("cancelled", "postponed", "future"):
        if flag(match.get(name)):
            return "not_applicable", name
    if not flag(match.get("finished")):
        return (("unknown", "awarded_unfinished") if flag(match.get("awarded")) else
                ("not_applicable", "unfinished"))
    return None


def classify_missing(match, winner, history, proof=None, ranking_unknown=False):
    """Classify absent *current scoped* content, not an ever-ingested row."""
    history = sorted(history, key=chronology)
    if ranking_unknown or (winner and not json_object(winner.get("capabilities_json"))[1]):
        return "unknown", "malformed_candidate_capabilities"
    disposition = applicability(match)
    if disposition:
        return disposition
    expected = {}
    if winner and winner.get("status") in SUCCESS:
        expected, valid = json_object(winner.get("expected_counts_json"))
        if not valid:
            return "unknown", "malformed_expected_counts"
    if winner and winner.get("status") in SUCCESS and (
            expected.get("match_payloads") == 1 or winner.get("physical_batch_present")):
        return "writer_failed", "expected_current_payload_missing"
    if history and (not winner or chronology(history[-1]) >= chronology(winner)):
        latest = history[-1]
        if parse_failure(latest):
            return "parse_failed", "schema_or_parser_failure"
        if latest.get("status") == "terminal_failure" or latest.get("fetch_outcome") == "terminal_failure":
            return "fetch_failed", "terminal_transport_failure"
        if latest.get("status") == "retryable_failure" or latest.get("fetch_outcome") == "retryable_failure":
            return "fetch_failed", "retryable_transport_failure"
    if winner and winner.get("status") in SUCCESS:
        return "unknown", "success_without_payload_expectation"
    success_time = max((chronology(row) for row in history if row.get("status") in SUCCESS),
                       default=(float("-inf"), ""))
    if proof and proof.get("valid") and tuple(proof["chronology"]) > success_time:
        return "source_unavailable", "explicit_scope_source_gap"
    unavailable = [row for row in history if is_v2(row) and row.get("status") == "not_available"
                   and row.get("error_code") == "source_data_not_found"
                   and chronology(row) > success_time and row.get("run_id")]
    runs = {row["run_id"] for row in unavailable}
    # The two observations establishing a spread must be from different runs.
    times = [(row["run_id"], timestamp(row.get("completed_at"))) for row in unavailable]
    spread = any(a_run != b_run and a_time is not None and b_time is not None and abs(a_time - b_time) >= 7200
                 for a_run, a_time in times for b_run, b_time in times)
    if len(runs) >= 3 or (len(runs) >= 2 and spread):
        return "source_unavailable", "repeated_source_data_not_found"
    return "unknown", "insufficient_absence_evidence"


def section_results(payload, finished, history=()):
    mask = payload.get("available_sections")
    if isinstance(mask, str):
        try:
            mask = json.loads(mask)
        except ValueError:
            mask = None
    mask_valid = ((isinstance(mask, list) and all(isinstance(item, str) for item in mask)) or
                  (isinstance(mask, dict) and all(isinstance(mask[name], bool) for name in SECTIONS if name in mask)))
    result = []
    for section in SECTIONS:
        typed, raw = payload.get("typed_" + section), payload.get("raw_" + section)
        available = (bool(mask.get(section)) if isinstance(mask, dict) else section in mask) if mask_valid else None
        if not flag(payload.get("payload_json_valid")) or typed is None or raw is None:
            category, reason = "unknown", "malformed_or_unresolved_json"
        elif raw and not typed:
            category, reason = "writer_failed", "raw_present_typed_absent"
        elif typed and not raw:
            category, reason = "unknown", "typed_present_raw_absent"
        elif not raw and not typed:
            observed = timestamp(payload.get("_observed_at"))
            schema_evidence = any(parse_failure(row) and (
                str(row.get("batch_id")) == str(payload.get("_target_batch_id")) or
                (observed is not None and chronology(row)[0] >= observed)) for row in history)
            if section == "matchFacts" and finished and schema_evidence:
                category, reason = "parse_failed", "matchFacts_schema_drift"
            else:
                # The raw bytes prove the source served no section content.
                # The stored mask may still claim presence: pre-#2302 batches
                # recorded the source's JSON false sentinel as an available
                # section (fixed in e46256b5). Raw evidence wins over the
                # stale mask, so this is a definite source omission, not an
                # unknown mask disagreement.
                category = "source_unavailable"
                reason = ("stored_source_omission_mask_stale"
                          if available is True else "stored_source_omission")
        elif not mask_valid:
            category, reason = "unknown", "invalid_available_mask"
        elif available != bool(typed):
            category, reason = "unknown", "available_mask_disagrees"
        else:
            category, reason = "confirmed_content", "raw_typed_mask_agree"
        result.append({"section": section, "available": available, "typed": typed,
                       "raw": raw, "category": category, "reason": reason})
    return result


def primary(counts):
    return next((name for name in PRIORITY if counts.get(name, 0)), "unknown")


def completion_row(row):
    return flag(row["finished"]) or flag(row["awarded"])


def mismatch_row(row, section_problem=False):
    return ((flag(row["finished"]) and not flag(row["payload_present"])) or
            row["category"] not in {"confirmed_content", "not_applicable"} or
            ((flag(row["finished"]) or flag(row["awarded"])) and flag(row["late_failure"])) or
            (completion_row(row) and section_problem))


def scope_completion_counts(summary, expected=None, status=None):
    counts = Counter()
    if summary is None:
        counts["unknown:unresolved_current_calendar"] += 1
        return counts
    state = summary["calendar_state"]
    if not int(summary["known"]) and status in SUCCESS and type(expected) is int and expected == 0:
        counts["not_applicable:advertised_empty_scope"] += 1
    elif state == "tombstone":
        counts["unknown:calendar_not_available_without_content_proof"] += 1
    elif state == "unknown" or not int(summary["known"]):
        counts["unknown:unresolved_current_calendar" if state == "unknown" else "unknown:empty_current_calendar"] += 1
    elif not int(summary["finished"]) and not summary["completion_reason_counts"]:
        counts["not_applicable:no_finished_matches"] += 1
    counts.update(summary["completion_reason_counts"])
    for section_reason, number in summary["section_reason_counts"].items():
        section, category, reason = section_reason.split(":", 2)
        counts[category + ":" + section + ":" + reason] += number
    return counts


def primary_reasons(counts):
    categories = Counter()
    for reason, number in counts.items():
        categories[reason.split(":", 1)[0]] += number
    return primary(categories)


def load_registry(path, required_scopes):
    scopes = {}
    for row in read_csv(path):
        if row["decision"] not in {"include", "included"}:
            continue
        cid = integer(row["competition_id"])
        # A delimiter separates exact labels; neither strip nor year conversion is valid.
        labels = row["required_season_keys"].split("|")
        if not labels or any(label == "" for label in labels):
            raise AuditError("included registry row has an empty required season label")
        for label in labels:
            key = (cid, label)
            if key in scopes:
                raise AuditError("duplicate required scope")
            scopes[key] = {"competition_id": cid, "source_season_key": label,
                           "catalog_name": row["catalog_name"], "required": True,
                           "marker_only": False, "historical_1294": False}
    if len(scopes) != required_scopes:
        raise AuditError(f"required scopes: expected {required_scopes}, got {len(scopes)}")
    return scopes


def load_historical(path):
    rows = read_csv(path)
    if len(rows) != 18:
        raise AuditError("historical input must contain exactly 18 rows")
    normalized = []
    for row in rows:
        season = row.get("source_season_key", row.get("season_key", row.get("season")))
        if season is None or season == "":
            raise AuditError("historical input lacks exact season key")
        gap = next((row[key] for key in ("historical_finished_without_details", "old_gap", "gap", "missing_payloads", "without_payload",
                                       "finished_without_payload", "old_finished_without_payload",
                                       "old_missing", "missing") if key in row), None)
        if gap is None:
            raise AuditError("historical input lacks old gap column")
        normalized.append({"competition_id": integer(row["competition_id"]),
                           "source_season_key": season, "old_gap": integer(gap, False),
                           "competition_name": row.get("competition_name", row.get("catalog_name", "")),
                           "historical_parser_and_plan_signatures": row.get("historical_parser_and_plan_signatures", ""),
                           "baseline_row": row})
    if len({scope_key(row) for row in normalized}) != 18 or sum(row["old_gap"] for row in normalized) != 1762:
        raise AuditError("historical keys/gap sum do not match the frozen #1294 input")
    return sorted(normalized, key=scope_key)


def marker_scopes(marker):
    """Strict decoder: undecodable evidence remains an explicit unknown target."""
    capabilities, valid = json_object(marker.get("capabilities_json"))
    coverage = capabilities.get("coverage")
    if not valid or not isinstance(coverage, dict):
        return [], False, False
    entities = coverage.get("scope_entities")
    # Common manifest form: family names plus the marker's own scope columns.
    if isinstance(entities, list) and all(isinstance(item, str) for item in entities):
        if "matches" not in entities:
            return [], True, False
        try:
            cid, season = scope_key(marker)
            if not isinstance(marker["source_season_key"], str) or not season:
                return [], False, True
            return [{"competition_id": cid, "source_season_key": season}], True, True
        except (KeyError, AuditError, TypeError):
            return [], False, True
    if not isinstance(entities, dict):
        return [], False, False
    if "matches" not in entities:
        return [], True, False
    matches = entities["matches"]
    if not isinstance(matches, list):
        return [], False, True
    scopes = []
    try:
        for item in matches:
            if not isinstance(item, dict):
                raise AuditError("invalid scope entity")
            cid, season = scope_key(item)
            if not isinstance(item["source_season_key"], str) or not season:
                raise AuditError("invalid scope season")
            scopes.append({"competition_id": cid, "source_season_key": season})
    except (KeyError, AuditError, TypeError):
        return [], False, True
    return sorted({scope_key(row): row for row in scopes}.values(), key=scope_key), bool(scopes), True


def marker_work_scopes(marker):
    """Retain a marker's physical scope even when its family evidence is malformed."""
    scopes = {scope_key(row): row for row in marker_scopes(marker)[0]}
    try:
        key = scope_key(marker)
        if isinstance(marker["source_season_key"], str) and key[1]:
            scopes[key] = {"competition_id": key[0], "source_season_key": key[1]}
    except (KeyError, AuditError, TypeError):
        pass
    return [scopes[key] for key in sorted(scopes)]


def decode_source_gaps(rows):
    """Decode the service's direct capabilities; scope comes from the manifest row."""
    proofs, anomalies = {}, []
    for row in rows:
        capabilities, valid = json_object(row.get("capabilities_json"))
        try:
            if not valid or capabilities.get("outcome") != "source_gap" or capabilities.get("reason") != "source_data_not_found":
                raise AuditError("unsupported source-gap capabilities")
            signature = capabilities["plan_signature"]
            if not isinstance(signature, str) or not signature or row.get("entity_id") != signature:
                raise AuditError("source-gap plan identity mismatch")
            cid, season = scope_key(row)
            if not isinstance(row["source_season_key"], str) or not season:
                raise AuditError("invalid proof season")
            entities = capabilities["scope_entities"]
            if (not isinstance(entities, list) or not entities or
                    not all(isinstance(item, str) and item for item in entities) or
                    len(set(entities)) != len(entities) or "matches" not in entities):
                raise AuditError("unsupported source-gap scope entities")
            for field in ("match_ids", "available_match_ids", "source_gap_matches"):
                if not isinstance(capabilities[field], list):
                    raise AuditError("invalid source-gap set")
            ids = [integer(value) for value in capabilities["match_ids"]]
            available = [integer(value) for value in capabilities["available_match_ids"]]
            missing = capabilities["source_gap_matches"]
            if (not ids or not missing or len(set(ids)) != len(ids) or
                    len(set(available)) != len(available)):
                raise AuditError("invalid source-gap ID set")
            completed = timestamp(row.get("completed_at"))
            if (row.get("status") != "not_available" or not is_v2(row) or completed is None or
                    not isinstance(row.get("batch_id"), str) or not row["batch_id"]):
                raise AuditError("invalid proof manifest")
            decoded = {}
            for proof in missing:
                if not isinstance(proof, dict):
                    raise AuditError("invalid per-match proof")
                mid = integer(proof["match_id"])
                target = proof["target_key"]
                uri, digest, run_id = proof["raw_uri"], proof["content_hash"], proof["run_id"]
                observed = timestamp(proof["observed_at"])
                if (mid in decoded or not isinstance(target, str) or
                        not re.fullmatch(r"[0-9a-fA-F]{64}", target) or
                        not isinstance(uri, str) or not re.fullmatch(r"s3://[^/\s]+/[^\s]+", uri) or
                        not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest) or
                        not isinstance(run_id, str) or not run_id.strip() or
                        observed is None or observed > completed or
                        proof["reason"] != "source_data_not_found"):
                    raise AuditError("invalid per-match source evidence")
                decoded[mid] = {"valid": True, "batch_id": row["batch_id"], "run_id": run_id,
                                "plan_signature": signature, "evidence": proof,
                                "chronology": [observed, str(row["batch_id"])],
                                "evidence_sha256": sha(canonical({"manifest": row, "proof": proof}).encode())}
            if set(available) & set(decoded) or set(available) | set(decoded) != set(ids):
                raise AuditError("source-gap sets are not a disjoint partition")
            for mid, value in decoded.items():
                key = (cid, season, mid)
                if key not in proofs or value["chronology"] > proofs[key]["chronology"]:
                    proofs[key] = value
        except (KeyError, ValueError, TypeError, AuditError):
            anomaly = {"batch_id": row.get("batch_id"), "reason": "unresolved_scope_source_gap",
                       "evidence_sha256": sha(canonical(row).encode())}
            try:
                anomaly["scope"] = list(scope_key(row))
            except (KeyError, TypeError, AuditError):
                pass
            anomalies.append(anomaly)
    return proofs, anomalies


def input_hashes(registry, historical):
    return {"code": {str(Path(__file__).resolve()): file_sha(__file__)},
            "sql": {path.name: file_sha(path) for path in sorted(SQL_DIR.glob("*.sql"))},
            "registry": {"path": str(Path(registry).resolve()), "sha256": file_sha(registry)},
            "historical": {"path": str(Path(historical).resolve()), "sha256": file_sha(historical)}}


FROZEN_FILES = ("snapshots.json", "markers.json", "source-gaps.json", "c1-inputs.json",
                "scope-plan.json", "input-hashes.json")


def seal(run):
    dump(run / "plan-seal.json", {name: file_sha(run / name) for name in FROZEN_FILES})


def check_frozen(run):
    saved = load(run / "plan-seal.json")
    if set(saved) != set(FROZEN_FILES) or any(file_sha(run / name) != value for name, value in saved.items()):
        raise AuditError("frozen plan changed; no automatic repinning is permitted")
    hashes = load(run / "input-hashes.json")
    current = input_hashes(hashes["registry"]["path"], hashes["historical"]["path"])
    if hashes != current:
        raise AuditError("code, SQL or input bytes changed; start a new run")
    snapshots = load(run / "snapshots.json")
    if set(snapshots) != {"matches", "payloads", "manifest", "c1"}:
        raise AuditError("missing snapshot")
    for row in snapshots.values():
        integer(row["snapshot_id"])
    if timestamp(load(run / "scope-plan.json").get("captured_at")) is None:
        raise AuditError("frozen plan lacks captured_at")
    return snapshots


def check_chunk_plan(run, snapshots):
    frozen = load(run / "chunk-plan.json")
    if file_sha(run / "chunk-plan.json") != load(run / "chunk-plan-seal.json")["sha256"]:
        raise AuditError("frozen chunk plan changed")
    plan_data = load(run / "scope-plan.json")
    scope_chunks = frozen["scope_chunks"]
    flattened = [row for group in scope_chunks for row in group]
    if (flattened != plan_data["scopes"] or not all(scope_chunks) or
            any(len(group) > LIMITS["scopes"] for group in scope_chunks) or
            frozen.get("snapshots") != snapshots or frozen.get("limits") != plan_data["limits"] or
            frozen.get("plan_sha256") != file_sha(run / "plan-seal.json") or
            frozen.get("bucket_count") != plan_data["limits"]["bucket_count"] or
            frozen.get("payload_modes") != payload_modes(scope_chunks, section_scope_keys(
                plan_data, load(run / "markers.json"))) or
            len(frozen["payload_batches"]) != len(scope_chunks)):
        raise AuditError("chunk plan does not match frozen scopes, markers and limits")
    for groups, limit in ((frozen["history_batches"], 500),
                          ([batch for group in frozen["payload_batches"] for batch in group], LIMITS["ids"])):
        for batch in groups:
            if not batch or len(batch) > limit or batch != sorted({integer(mid) for mid in batch}):
                raise AuditError("invalid frozen ID chunk")
    return frozen


def plan(args, transport=None):
    captured_at = datetime.now(timezone.utc).isoformat()
    run = Path(args.run_dir).resolve()
    if run.exists() and any(run.iterdir()):
        raise AuditError("plan requires an empty run directory; snapshots cannot be replaced")
    registry = load_registry(args.registry, args.required_scopes)
    historical = load_historical(args.historical)
    run.mkdir(parents=True, exist_ok=True)
    queries = Queries(transport or RemoteTrino(validate_name(run.name)), run / "query-ledger.csv")
    rows = queries.run(render("snapshots"), "snapshots")
    snapshots = {}
    for row in rows:
        key = row["table_key"]
        if key in snapshots:
            raise AuditError("ambiguous latest snapshot")
        snapshots[key] = {"snapshot_id": integer(row["snapshot_id"]),
                          "made_current_at": row["made_current_at"]}
    if set(snapshots) != {"matches", "payloads", "manifest", "c1"}:
        raise AuditError("all four latest snapshots are required")
    dump(run / "snapshots.json", snapshots)
    markers = sorted(queries.run(render("markers", snapshots), "markers"), key=lambda row: row["target_key"])
    if len({row["target_key"] for row in markers}) != len(markers):
        raise AuditError("duplicate latest marker target")
    dump(run / "markers.json", markers)
    gaps = queries.run(render("source_gaps", snapshots), "source_gaps")
    dump(run / "source-gaps.json", sorted(gaps, key=canonical))
    c1 = queries.run(render("c1_inputs", snapshots), "c1_inputs")
    dump(run / "c1-inputs.json", sorted(c1, key=canonical))
    for marker in markers:
        scopes = marker_work_scopes(marker)
        for row in scopes:
            key = scope_key(row)
            if key not in registry:
                registry[key] = {**row, "catalog_name": "", "required": False,
                                 "marker_only": True, "historical_1294": False}
    for row in historical:
        key = scope_key(row)
        if key not in registry:
            registry[key] = {"competition_id": key[0], "source_season_key": key[1],
                             "catalog_name": row["competition_name"],
                             "required": False, "marker_only": False, "historical_1294": True}
        registry[key]["historical_1294"] = True
    scopes = [registry[key] for key in sorted(registry)]
    limits = {**LIMITS, "bucket_count": integer(getattr(args, "bucket_count", 16))}
    dump(run / "scope-plan.json", {"scopes": scopes, "historical": historical, "captured_at": captured_at,
                                    "required_scopes": args.required_scopes, "limits": limits,
                                    "initial_chunks": chunks(scopes, LIMITS["scopes"])})
    dump(run / "input-hashes.json", input_hashes(args.registry, args.historical))
    seal(run)
    return run


def validate_templates(snapshots):
    parameters = {"scope_predicate": "FALSE", "manifest_scope_predicate": "FALSE", "candidate_batch_predicate": "FALSE",
                  "selected_batch_predicate": "FALSE", "match_predicate": "TRUE",
                  "match_id_predicate": "FALSE", "bucket": 0, "bucket_count": 16}
    templates = sorted(SQL_DIR.glob("*.sql"))
    if not {"current_payloads", "current_payload_ids", "league_manifest"} <= {path.stem for path in templates}:
        raise AuditError("required payload/manifest template missing")
    for path in templates:
        guard_sql(render(path.stem, snapshots, **parameters))


def read_scope_chunk(queries, snapshots, scopes, label):
    pred = scope_predicate(scopes)
    candidates = queries.run(render("league_manifest", snapshots, scope_predicate=pred,
                                     manifest_scope_predicate=manifest_scope_predicate(scopes)),
                             "league_manifest", chunk=label)
    presence = set()
    # Candidate lists can be much longer than the scope list. Bound their IN predicates too.
    for index, batches in enumerate(chunks(sorted({str(row["batch_id"]) for row in candidates
                                                  if row["status"] in SUCCESS}), 500)):
        rows = queries.run(render("match_batch_presence", snapshots, scope_predicate=pred,
                                  candidate_batch_predicate=in_predicate("_target_batch_id", batches)),
                           "match_batch_presence", chunk=f"{label}.{index}")
        presence.update((*scope_key(row), str(row["_target_batch_id"])) for row in rows)
    winners, anomalies = choose_calendar(candidates, presence)
    selected = {key: row for key, row in winners.items() if row["status"] in SUCCESS}
    matches = []
    if selected:
        matches = queries.run(render("current_matches", snapshots, scope_predicate=pred,
                                     selected_batch_predicate=in_predicate(
                                         "_target_batch_id", [row["batch_id"] for row in selected.values()]),
                                     match_predicate="TRUE"), "current_matches", chunk=label)
        matches = [row for row in matches if scope_key(row) in selected and str(row["_target_batch_id"])
                   == str(selected[scope_key(row)]["batch_id"])]
    return winners, deduplicate(matches), anomalies


def fetch_scopes(queries, snapshots, scope_chunks, calibrate):
    pending = list(scope_chunks)
    final, calendars, current, anomalies = [], {}, {}, []
    while pending:
        scopes = pending.pop(0)
        label = str(len(final))
        try:
            selected, rows, found = read_scope_chunk(queries, snapshots, scopes, label)
        except QueryError as exc:
            splittable = exc.error_class in {"SQLTooLarge", "ClientTimeout", "StatementBudgetExceeded",
                                             "ServerTimeout", "ReadTimeout"}
            if not calibrate or not splittable or len(scopes) <= 1:
                raise
            middle = len(scopes) // 2
            pending[:0] = [scopes[:middle], scopes[middle:]]
            continue
        final.append(scopes)
        calendars.update(selected)
        current.update(rows)
        anomalies.extend(found)
    return final, calendars, current, anomalies


def payload_modes(scope_chunks, section_scopes):
    return ["current_payloads" if any(scope_key(row) in section_scopes for row in scopes)
            else "current_payload_ids" for scopes in scope_chunks]


def section_scope_keys(plan_data, markers):
    return ({scope_key(row) for row in plan_data["historical"]} |
            {scope_key(row) for marker in markers for row in marker_work_scopes(marker)})


def fetch_payload_batch(queries, snapshots, scopes, ids, winners, label, mode):
    if mode not in {"current_payloads", "current_payload_ids"}:
        raise AuditError("invalid payload mode")
    selected = {mid: winners[mid] for mid in ids if mid in winners and winners[mid]["status"] in SUCCESS}
    if not selected:
        return []
    return queries.run(render(mode, snapshots, scope_predicate=scope_predicate(scopes),
                              manifest_scope_predicate=manifest_scope_predicate(scopes),
                              selected_batch_predicate=in_predicate("_target_batch_id", [
                                  row["batch_id"] for row in selected.values()]),
                              match_id_predicate=in_predicate("match_id", list(selected), numeric=True)),
                       mode, chunk=label)


def fetch_payloads(queries, snapshots, scope_chunks, calendar, winners, frozen, calibrate, modes):
    if len(modes) != len(scope_chunks) or (not calibrate and len(frozen) != len(scope_chunks)):
        raise AuditError("payload plan length mismatch")
    result, batch_plan = [], []
    for index, scopes in enumerate(scope_chunks):
        keys = {scope_key(row) for row in scopes}
        # Include unfinished known rows: they are required for full known/payload evidence
        # and for sections in marker scopes. Finished rows retain their literal subset.
        ids = sorted({key[2] for key in calendar if key[:2] in keys})
        pending = chunks(ids, LIMITS["ids"]) if calibrate else frozen[index]
        if not calibrate and sorted(mid for batch in pending for mid in batch) != ids:
            raise AuditError("current IDs differ from calibrated payload chunks")
        accepted = []
        pending = list(pending)
        while pending:
            batch = pending.pop(0)
            try:
                rows = fetch_payload_batch(queries, snapshots, scopes, batch, winners,
                                           f"{index}.{len(accepted)}", modes[index])
            except QueryError as exc:
                if not calibrate or len(batch) <= 1 or exc.error_class not in {
                        "SQLTooLarge", "ClientTimeout", "StatementBudgetExceeded", "ReadTimeout",
                        "ServerTimeout"}:
                    raise
                middle = len(batch) // 2
                pending[:0] = [batch[:middle], batch[middle:]]
                continue
            accepted.append(batch)
            result.extend(rows)
        batch_plan.append(accepted)
    return current_payloads(result, calendar, winners), batch_plan


def fetch_history(queries, snapshots, ids, frozen, calibrate):
    pending = chunks(sorted(ids), 500) if calibrate else frozen
    if not calibrate and sorted(mid for batch in pending for mid in batch) != sorted(ids):
        raise AuditError("history IDs differ from calibrated chunks")
    pending, accepted, history = list(pending), [], defaultdict(list)
    while pending:
        batch = pending.pop(0)
        try:
            rows = queries.run(render("match_evidence", snapshots,
                                      match_id_predicate=in_predicate("entity_id", [str(mid) for mid in batch])),
                               "match_evidence", chunk=str(len(accepted)))
        except QueryError as exc:
            if not calibrate or len(batch) <= 1 or exc.error_class not in {
                    "SQLTooLarge", "ClientTimeout", "StatementBudgetExceeded", "ReadTimeout",
                    "ServerTimeout"}:
                raise
            middle = len(batch) // 2
            pending[:0] = [batch[:middle], batch[middle:]]
            continue
        accepted.append(batch)
        for row in rows:
            history[integer(row["entity_id"])].append(row)
    return history, accepted


def reconcile_c1(c1_rows, evidence, snapshots, captured_at=None):
    """C1 is a scoped cache of inputs, never a global current-content inventory."""
    current = {row["scoped_id"] for row in evidence if flag(row["payload_present"])}
    by_id = {row["scoped_id"]: row for row in evidence}
    known = set(by_id)
    inputs, controls, unresolved = defaultdict(list), [], []
    unresolved_ids = set()
    deadline_counts = Counter()
    for row in sorted(c1_rows, key=canonical):
        if row.get("kind") != "input":
            (controls if row.get("kind") else unresolved).append(row)
            continue
        try:
            if not isinstance(row["source_season_key"], str) or not row["source_season_key"]:
                raise AuditError("invalid C1 season")
            sid = scoped_id(row)
            deadline_day = str(row["deadline_day"])
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", deadline_day):
                raise AuditError("invalid C1 deadline day")
            deadline = timestamp(row["deadline"])
            if deadline is None:
                raise AuditError("invalid C1 deadline")
            na_runs = integer(row["na_runs"], False)
            success_count = integer(row["success_count"], False)
            spread = float(row["na_spread_s"])
            if not 0 <= spread < float("inf"):
                raise AuditError("invalid C1 spread")
            ingested, ft = row["first_ingested"], row["first_ft"]
            if any(value not in (None, "") and timestamp(value) is None for value in (ingested, ft)):
                raise AuditError("invalid C1 observation time")
            inputs[sid].append({"first_ingested": ingested, "first_ft": ft,
                                "deadline_day": deadline_day, "deadline": row["deadline"],
                                "na_runs": na_runs, "na_spread_s": spread, "success_count": success_count,
                                "deadline_reached": timestamp(captured_at) is not None
                                and deadline <= timestamp(captured_at)})
            deadline_counts[deadline_day] += 1
        except (KeyError, TypeError, ValueError, AuditError):
            unresolved.append(row)
            try:
                if isinstance(row["source_season_key"], str) and row["source_season_key"]:
                    unresolved_ids.add(scoped_id(row))
            except (KeyError, TypeError, AuditError):
                pass
    c1_ids = set(inputs)
    c1_present = {sid for sid, rows in inputs.items() if any(row["first_ingested"] not in (None, "") for row in rows)}
    c1_finished = {sid for sid, rows in inputs.items() if any(row["first_ft"] not in (None, "") for row in rows)}
    unavailable = {sid for sid, rows in inputs.items() if sid in c1_finished - c1_present and
                   not any(row["success_count"] for row in rows) and any(
                       row["na_runs"] >= 3 or (row["na_runs"] >= 2 and row["na_spread_s"] >= 7200)
                       for row in rows)}
    comparable = c1_ids & known
    def has_evidence(row):
        ids = row["evidence_ids"]
        return bool(json.loads(ids) if isinstance(ids, str) else ids) or flag(row["payload_present"])

    sets = {"cached_input_scoped_ids": c1_ids,
            "unresolved_input_scoped_ids": unresolved_ids,
            "first_ft_known_finished_scoped_ids": c1_finished,
            "finished_without_ever_ingested_scoped_ids": c1_finished - c1_present,
            "source_unavailable_threshold_scoped_ids": unavailable,
            "c1_unconfirmed_scoped_ids": (c1_finished - c1_present) - unavailable,
            "c1_no_absence_evidence_scoped_ids": {sid for sid in c1_finished - c1_present
                                                if not any(row["na_runs"] for row in inputs[sid])},
            "deadline_reached_scoped_ids": {sid for sid, rows in inputs.items() if any(row["deadline_reached"] for row in rows)},
            "unconfirmed_scoped_ids": {sid for sid in comparable if by_id[sid]["category"] not in {"confirmed_content", "not_applicable"}},
            "no_evidence_scoped_ids": unresolved_ids | (c1_ids - known) | {sid for sid in comparable
                                                        if not has_evidence(by_id[sid])},
            "outside_audit_calendar_ids": c1_ids - known,
            "c1_no_payload_ids": c1_ids - c1_present,
            "late_correction_current_added_ids": (comparable - c1_present) & current,
            "ever_ingested_but_not_current_ids": (comparable & c1_present) - current}
    return {"scope": "limited C1 kind=input scoped cohort; ever-ingested versus current, not whole history",
            "c1_snapshot": snapshots["c1"]["snapshot_id"], "audit_snapshots": snapshots,
            "captured_at": captured_at, "input_rows": sum(map(len, inputs.values())),
            "cached_input_rows": sum(row.get("kind") == "input" for row in c1_rows),
            "distinct_input_ids": len(c1_ids), "distinct_input_global_ids": len({json.loads(sid)[2] for sid in c1_ids}),
            "control_rows": len(controls), "unresolved_rows": len(unresolved),
            "deadline_days_counts": dict(sorted(deadline_counts.items())),
            "observations_by_scoped_id": dict(sorted(inputs.items())),
            "c1_no_payload": len(c1_ids - c1_present),
            "first_ft_known_finished": len(c1_finished),
            "finished_without_ever_ingested": len(c1_finished - c1_present),
            "source_unavailable_threshold": len(unavailable),
            "current_no_payload_comparable": len(comparable - current),
            **{key: sorted(values) for key, values in sets.items()},
            "set_hashes": {key: id_set_hash(values) for key, values in sets.items()},
            "c1_id_set_hash": id_set_hash(c1_ids),
            "c1_no_payload_id_set_hash": id_set_hash(c1_ids - c1_present),
            "controls_sha256": sha(canonical(sorted(controls, key=canonical)).encode()),
            "unresolved_sha256": sha(canonical(sorted(unresolved, key=canonical)).encode())}


def make_artifacts(plan_data, markers, source_gaps, c1, snapshots, calendars, calendar,
                   winners, payloads, histories, anomalies):
    scopes = {scope_key(row): row for row in plan_data["scopes"]}
    section_keys = section_scope_keys(plan_data, markers)
    proofs, gap_anomalies = decode_source_gaps(source_gaps)
    anomalies = list(anomalies) + gap_anomalies
    suspect = {tuple(row["scope"]) for row in anomalies if row.get("scope") and
               row["reason"] == "malformed_candidate_capabilities"}
    gap_suspect = {tuple(row["scope"]) for row in gap_anomalies if row.get("scope")}
    evidence, mismatches, sections = [], [], []
    by_scope = defaultdict(list)
    section_counts = defaultdict(Counter)
    for key, match in sorted(calendar.items()):
        scope = scopes[key[:2]]
        payload, winner = payloads.get(key), winners.get(key[2])
        history = histories.get(key[2], [])
        ranking_unknown = key[:2] in suspect or (winner and (
            flag(winner.get("ranking_unknown")) or not json_object(winner.get("capabilities_json"))[1]))
        latest = max(history, key=chronology, default={})
        late = bool(winner and (flag(winner.get("late_failure")) or (latest and
                    chronology(latest) > chronology(winner) and latest.get("status") not in SUCCESS)))
        if ranking_unknown:
            category, reason = "unknown", "malformed_candidate_capabilities"
        elif applicability(match):
            category, reason = applicability(match)
        elif payload is not None:
            category, reason = "confirmed_content", "current_payload_present"
        elif key[:2] in gap_suspect:
            category, reason = "unknown", "unresolved_scope_source_gap"
        else:
            category, reason = classify_missing(match, winner, history, proofs.get(key), ranking_unknown)
        proof = proofs.get(key)
        evidence_rows = sorted(history or ([winner] if winner else []), key=canonical)
        ids = [str(row.get("batch_id", "")) for row in evidence_rows]
        if proof:
            ids.append(str(proof["batch_id"]))
        row = {"competition_id": key[0], "source_season_key": key[1], "match_id": key[2],
               "scoped_id": scoped_id(match), "finished": flag(match.get("finished")),
               "cancelled": flag(match.get("cancelled")), "postponed": flag(match.get("postponed")),
               "awarded": flag(match.get("awarded")), "future": flag(match.get("future")),
               "calendar_batch": match["_target_batch_id"], "calendar_observed_at": match.get("_observed_at"),
               "payload_present": payload is not None,
               "payload_batch": payload.get("_target_batch_id") if payload else "",
               "payload_observed_at": payload.get("_observed_at") if payload else "",
               "required": scope["required"], "marker_only": scope["marker_only"],
               "historical_1294": scope["historical_1294"], "category": category, "reason": reason,
               "late_failure": late, "evidence_ids": sorted(set(ids)),
               "evidence_sha256": sha(canonical({"manifest": evidence_rows, "proof": proof}).encode())}
        evidence.append(row)
        by_scope[key[:2]].append(row)
        section_problem = False
        if payload and key[:2] in section_keys:
            for result in section_results(payload, row["finished"], history):
                if completion_row(row) and row["category"] != "not_applicable":
                    section_counts[key[:2]][result["section"] + ":" + result["category"] + ":" + result["reason"]] += 1
                section_problem |= result["category"] != "confirmed_content"
                # All six comparisons are saved, including matching sections.
                sections.append({"competition_id": key[0], "source_season_key": key[1], "match_id": key[2],
                                 **result, "payload_batch": payload["_target_batch_id"]})
        if mismatch_row(row, section_problem):
            mismatches.append(row)
    summaries = []
    for key, scope in sorted(scopes.items()):
        rows = by_scope[key]
        finished = [row for row in rows if row["finished"]]
        missing = [row for row in finished if not row["payload_present"]]
        selected = calendars.get(key)
        state = "unknown" if not selected or key in suspect else (
            "tombstone" if selected["status"] == "not_available" else "current")
        summaries.append({**scope, "calendar_state": state, "known": len(rows), "finished": len(finished),
                          "with_payload": sum(row["payload_present"] for row in finished),
                          "without_payload": len(missing),
                          "actionable_gap": sum(row["category"] != "not_applicable" for row in missing),
                          "payload_rows": sum(row["payload_present"] for row in rows),
                          "known_id_set_hash": id_set_hash(row["scoped_id"] for row in rows),
                          "payload_id_set_hash": id_set_hash(row["scoped_id"] for row in rows if row["payload_present"]),
                          "mismatch_id_set_hash": id_set_hash(row["scoped_id"] for row in mismatches
                                                              if scope_key(row) == key),
                          "reason_counts": dict(sorted(Counter(row["category"] + ":" + row["reason"] for row in rows).items())),
                          "completion_reason_counts": dict(sorted(Counter(row["category"] + ":" + row["reason"]
                                                                          for row in rows if completion_row(row)).items())),
                          "section_reason_counts": dict(sorted(section_counts[key].items()))})
    summary_by_key = {scope_key(row): row for row in summaries}
    marker_results = []
    for marker in markers:
        selected_scopes, valid, has_matches = marker_scopes(marker)
        expected, expected_valid = json_object(marker.get("expected_counts_json"))
        capabilities, _ = json_object(marker.get("capabilities_json"))
        counts = Counter()
        if not valid:
            counts["unknown:malformed_marker_scope_evidence"] += 1
        elif not has_matches:
            counts["not_applicable:no_matches_family"] += 1
        else:
            for selected in selected_scopes:
                key = scope_key(selected)
                summary = summary_by_key.get(key)
                counts.update(scope_completion_counts(summary, expected.get("matches"), marker.get("status")))
                if key in suspect:
                    counts["unknown:malformed_candidate_capabilities"] += 1
        if not expected_valid:
            counts["unknown:malformed_expected_counts"] += 1
        if marker.get("status") not in SUCCESS | {"not_available"}:
            counts["unknown:latest_marker_incomplete"] += 1
        cohorts = sorted({name for selected in marker_work_scopes(marker)
                          for name in ("required", "marker_only", "historical_1294")
                          if scopes.get(scope_key(selected), {}).get(name)})
        marker_results.append({"competition_id": marker.get("competition_id", ""),
                               "source_season_key": marker.get("source_season_key", ""),
                               "plan_signature": capabilities.get("plan_signature", marker.get("entity_id", "")),
                               "entity_id": marker.get("entity_id", ""), "cohort": cohorts,
                               "target_key": marker["target_key"], "batch_id": marker["batch_id"],
                               "status": marker["status"], "category": primary_reasons(counts),
                               "reason_counts": dict(sorted(counts.items())),
                               "scope_keys": [list(scope_key(row)) for row in selected_scopes],
                               "evidence_sha256": sha(canonical(marker).encode())})
    historical = [{"competition_id": row["competition_id"], "source_season_key": row["source_season_key"],
                   "competition_name": row["competition_name"],
                   "historical_parser_and_plan_signatures": row["historical_parser_and_plan_signatures"],
                   "baseline_date": "2026-10-21", "old_gap": row["old_gap"],
                   "current_finished": summary_by_key[scope_key(row)]["finished"],
                   "current_with_payload": summary_by_key[scope_key(row)]["with_payload"],
                   "current_without_payload": summary_by_key[scope_key(row)]["without_payload"],
                   "current_actionable_gap": summary_by_key[scope_key(row)]["actionable_gap"],
                   "calendar_state": summary_by_key[scope_key(row)]["calendar_state"],
                   "current_primary_outcome": primary_reasons(scope_completion_counts(summary_by_key[scope_key(row)])),
                   "current_completion_reason_counts": summary_by_key[scope_key(row)]["completion_reason_counts"],
                   "current_section_reason_counts": summary_by_key[scope_key(row)]["section_reason_counts"],
                   "current_mismatch_id_set_hash": summary_by_key[scope_key(row)]["mismatch_id_set_hash"]}
                  for row in plan_data["historical"]]
    return {"current-id-evidence.csv": (ID_FIELDS, evidence),
            "mismatched-ids.csv": (MISMATCH_FIELDS, mismatches),
            "section-evidence.csv": (SECTION_FIELDS, sections),
            "section-mismatches.csv": (SECTION_FIELDS, [row for row in sections if row["category"] != "confirmed_content"]),
            "scope-summary.csv": (SUMMARY_FIELDS, summaries),
            "marker-results.csv": (MARKER_FIELDS, marker_results),
            "historical-1294-results.csv": (tuple(historical[0]), historical),
            "c1-reconciliation.json": reconcile_c1(c1, evidence, snapshots, plan_data["captured_at"]),
            "candidate-anomalies.json": sorted(anomalies, key=canonical)}


def collect(args, transport=None):
    run = Path(args.run_dir).resolve()
    pass_id = validate_name(args.pass_id)
    snapshots = check_frozen(run)
    validate_templates(snapshots)
    plan_data = load(run / "scope-plan.json")
    frozen_path = run / "chunk-plan.json"
    calibrate = bool(args.calibrate)
    if calibrate and frozen_path.exists():
        raise AuditError("calibration already frozen; start a new run to change chunks")
    if not calibrate and not frozen_path.exists():
        raise AuditError("run a calibration pass before collecting frozen passes")
    frozen = load(frozen_path) if not calibrate else {
        "scope_chunks": plan_data["initial_chunks"], "bucket_count": plan_data["limits"]["bucket_count"]}
    if not calibrate:
        frozen = check_chunk_plan(run, snapshots)
    destination = run / "passes" / pass_id
    destination.mkdir(parents=True, exist_ok=False)
    queries = Queries(transport or RemoteTrino(validate_name(run.name)), destination / "query-ledger.csv",
                      budget=LIMITS["pass_seconds"])
    status = {"complete": False, "calibration": calibrate, "snapshots": snapshots,
              "plan_sha256": file_sha(run / "plan-seal.json"),
              "input_hashes": load(run / "input-hashes.json")}
    dump(destination / "status.json", status)
    try:
        scope_chunks, calendars, calendar, anomalies = fetch_scopes(
            queries, snapshots, frozen["scope_chunks"], calibrate)
        audit_time = timestamp(plan_data.get("captured_at"))
        if audit_time is None:
            raise AuditError("scope plan has no usable captured_at")
        for match in calendar.values():
            kickoff = timestamp(match.get("utc_time"))
            match["future"] = kickoff is not None and kickoff + 3 * 3600 > audit_time
        markers = load(run / "markers.json")
        section_scopes = section_scope_keys(plan_data, markers)
        modes = payload_modes(scope_chunks, section_scopes)
        if not calibrate and frozen.get("payload_modes") != modes:
            raise AuditError("payload modes differ from frozen scope/marker plan")
        winners = {}
        for bucket in range(integer(frozen["bucket_count"])):
            rows = queries.run(render("match_manifest", snapshots, bucket=bucket,
                                      bucket_count=frozen["bucket_count"]), "match_manifest", bucket=bucket)
            for row in rows:
                mid = integer(row["entity_id"])
                if mid in winners:
                    raise AuditError("duplicate global match winner across CRC buckets")
                winners[mid] = row
        payloads, payload_batches = fetch_payloads(
            queries, snapshots, scope_chunks, calendar, winners, frozen.get("payload_batches"), calibrate, modes)
        history_ids = {key[2] for key, match in calendar.items() if key not in payloads and
                       (flag(match.get("finished")) or flag(match.get("awarded")))}
        for key, payload in payloads.items():
            if key[:2] in section_scopes and (flag(calendar[key].get("finished")) or
                                              flag(calendar[key].get("awarded"))):
                results = section_results(payload, flag(calendar[key].get("finished")))
                if any(row["category"] == "unknown" or (row["section"] == "matchFacts" and
                        row["category"] != "confirmed_content") for row in results):
                    history_ids.add(key[2])
        histories, history_batches = fetch_history(
            queries, snapshots, history_ids, frozen.get("history_batches"), calibrate)
        if calibrate:
            frozen = {"scope_chunks": scope_chunks, "bucket_count": frozen["bucket_count"],
                      "payload_modes": modes,
                      "payload_batches": payload_batches, "history_batches": history_batches,
                      "limits": plan_data["limits"], "snapshots": snapshots,
                      "plan_sha256": status["plan_sha256"]}
            dump(frozen_path, frozen)
            dump(run / "chunk-plan-seal.json", {"sha256": file_sha(frozen_path)})
        artifacts = make_artifacts(plan_data, markers, load(run / "source-gaps.json"),
                                   load(run / "c1-inputs.json"), snapshots, calendars, calendar,
                                   winners, payloads, histories, anomalies)
        for name, content in artifacts.items():
            if name.endswith(".csv"):
                write_csv(destination / name, content[0], content[1])
            else:
                dump(destination / name, content)
        check_frozen(run)
        elapsed = time.monotonic() - queries.started
        if elapsed > LIMITS["pass_seconds"]:
            raise AuditError(f"pass exceeded {LIMITS['pass_seconds']} seconds")
        status.update(complete=True, chunk_plan_sha256=file_sha(frozen_path))
        dump(destination / "content-hashes.json", {name: file_sha(destination / name) for name in CONTENT_FILES})
        dump(destination / "status.json", status)
    except Exception as exc:
        status.update(complete=False, error_class=type(exc).__name__)
        dump(destination / "status.json", status)
        raise
    finally:
        dump(destination / "timing.json", {"pass_wall_seconds": round(time.monotonic() - queries.started, 6),
                                           "query_count": len(queries.rows),
                                           "max_statement_seconds": max((row["wall_seconds"] for row in queries.rows), default=0)})
    return destination


def checked_pass(run, pass_id):
    snapshots = check_frozen(run)
    check_chunk_plan(run, snapshots)
    directory = run / "passes" / validate_name(pass_id)
    status = load(directory / "status.json")
    if not status.get("complete") or status.get("calibration"):
        raise AuditError("a complete non-calibration pass is required")
    if (status["snapshots"] != snapshots or status["plan_sha256"] != file_sha(run / "plan-seal.json")
            or status["input_hashes"] != load(run / "input-hashes.json")
            or status["chunk_plan_sha256"] != file_sha(run / "chunk-plan.json")
            or status["chunk_plan_sha256"] != load(run / "chunk-plan-seal.json")["sha256"]):
        raise AuditError("pass does not use the frozen inputs, snapshots and chunks")
    hashes = load(directory / "content-hashes.json")
    if set(hashes) != set(CONTENT_FILES) or any(file_sha(directory / name) != value for name, value in hashes.items()):
        raise AuditError("pass content hash mismatch")
    return directory, status


def csv_bool(value):
    if value not in {"0", "1"}:
        raise AuditError("invalid canonical CSV boolean")
    return value == "1"


def validate_report(run, directory):
    plan_data = load(run / "scope-plan.json")
    evidence = read_csv(directory / "current-id-evidence.csv")
    summaries = read_csv(directory / "scope-summary.csv")
    markers = read_csv(directory / "marker-results.csv")
    historical = read_csv(directory / "historical-1294-results.csv")
    mismatches = read_csv(directory / "mismatched-ids.csv")
    sections = read_csv(directory / "section-evidence.csv")
    section_mismatches = read_csv(directory / "section-mismatches.csv")
    ledger = read_csv(directory / "query-ledger.csv")
    initial_ledger = read_csv(run / "query-ledger.csv")
    timing = load(directory / "timing.json")
    checks = {}
    for name, fields in (("current-id-evidence.csv", ID_FIELDS), ("mismatched-ids.csv", MISMATCH_FIELDS),
                         ("section-evidence.csv", SECTION_FIELDS), ("section-mismatches.csv", SECTION_FIELDS),
                         ("marker-results.csv", MARKER_FIELDS), ("scope-summary.csv", SUMMARY_FIELDS)):
        with (directory / name).open(encoding="utf-8", newline="") as stream:
            checks["columns_" + name] = next(csv.reader(stream), []) == list(fields)
    expected_scopes = {scope_key(row) for row in plan_data["scopes"]}
    actual_scopes = [scope_key(row) for row in summaries]
    checks["all_scopes_once"] = len(actual_scopes) == len(set(actual_scopes)) and set(actual_scopes) == expected_scopes
    required = {scope_key(row) for row in plan_data["scopes"] if row["required"]}
    checks["all_required_scopes_once"] = ({scope_key(row) for row in summaries if csv_bool(row["required"])} == required
                                           and len(required) == plan_data["required_scopes"])
    expected_markers = {row["target_key"] for row in load(run / "markers.json")}
    checks["all_latest_marker_targets_once"] = (len(markers) == len(expected_markers) and
                                               {row["target_key"] for row in markers} == expected_markers)
    historical_keys = {scope_key(row) for row in plan_data["historical"]}
    checks["historical_18_once"] = len(historical) == 18 and {scope_key(row) for row in historical} == historical_keys
    checks["historical_baseline_preserved"] = (sum(int(row["old_gap"]) for row in historical) == 1762
                                              and all(row["baseline_date"] == "2026-10-21" for row in historical)
                                              and all(any(scope_key(old) == scope_key(row) and
                                                  int(row["old_gap"]) == old["old_gap"] and
                                                  row["competition_name"] == old["competition_name"] and
                                                  row["historical_parser_and_plan_signatures"] == old["historical_parser_and_plan_signatures"]
                                                  for old in plan_data["historical"]) for row in historical))
    checks["finished_arithmetic"] = all(int(row["finished"]) == int(row["with_payload"]) + int(row["without_payload"])
                                         for row in summaries)
    ids = [row["scoped_id"] for row in evidence]
    checks["unique_canonical_ids"] = (len(ids) == len(set(ids)) and
                                       all(row["scoped_id"] == scoped_id(row) for row in evidence))
    checks["canonical_flags_and_categories"] = all(
        row["category"] in PRIORITY and all(csv_bool(row[field]) in {False, True} for field in
            ("finished", "cancelled", "postponed", "awarded", "future", "payload_present", "required",
             "marker_only", "historical_1294", "late_failure")) for row in evidence)
    planned_scopes = {scope_key(row): row for row in plan_data["scopes"]}
    checks["cohort_flags_reconcile"] = all(scope_key(row) in planned_scopes and all(
        csv_bool(row[field]) == planned_scopes[scope_key(row)][field]
        for field in ("required", "marker_only", "historical_1294")) for row in evidence + summaries)
    by_scope = defaultdict(list)
    for row in evidence:
        by_scope[scope_key(row)].append(row)
    checks["only_planned_scope_rows"] = set(by_scope) <= expected_scopes
    reconcile = True
    relevant_sections = defaultdict(Counter)
    evidence_map = {row["scoped_id"]: row for row in evidence}
    for row in sections:
        parent = evidence_map.get(scoped_id(row))
        if parent and completion_row(parent) and parent["category"] != "not_applicable":
            relevant_sections[scope_key(row)][row["section"] + ":" + row["category"] + ":" + row["reason"]] += 1
    for summary in summaries:
        rows = by_scope[scope_key(summary)]
        finished = [row for row in rows if csv_bool(row["finished"])]
        missing = [row for row in finished if not csv_bool(row["payload_present"])]
        reconcile &= (int(summary["known"]) == len(rows) and int(summary["finished"]) == len(finished)
                      and int(summary["without_payload"]) == len(missing)
                      and int(summary["actionable_gap"]) == sum(row["category"] != "not_applicable" for row in missing)
                      and int(summary["payload_rows"]) == sum(csv_bool(row["payload_present"]) for row in rows)
                      and summary["known_id_set_hash"] == id_set_hash(row["scoped_id"] for row in rows)
                      and summary["payload_id_set_hash"] == id_set_hash(row["scoped_id"] for row in rows
                                                                                   if csv_bool(row["payload_present"]))
                      and json.loads(summary["reason_counts"]) == dict(Counter(row["category"] + ":" + row["reason"] for row in rows))
                      and json.loads(summary["completion_reason_counts"]) == dict(Counter(
                          row["category"] + ":" + row["reason"] for row in rows if completion_row(row)))
                      and json.loads(summary["section_reason_counts"]) == dict(relevant_sections[scope_key(summary)]))
    checks["scope_counts_and_id_hashes"] = bool(reconcile)
    section_problem_ids = {scoped_id(row) for row in sections if row["category"] != "confirmed_content"}
    expected_mismatches = [row for row in evidence if mismatch_row(row, row["scoped_id"] in section_problem_ids)]
    checks["mismatch_rows_reconcile"] = mismatches == expected_mismatches
    mismatch_by_scope = defaultdict(list)
    for row in mismatches:
        mismatch_by_scope[scope_key(row)].append(row["scoped_id"])
    checks["mismatch_hashes_reconcile"] = all(row["mismatch_id_set_hash"] == id_set_hash(mismatch_by_scope[scope_key(row)])
                                              for row in summaries)
    summary_map = {scope_key(row): {**row, **{field: json.loads(row[field]) for field in
                   ("reason_counts", "completion_reason_counts", "section_reason_counts")}} for row in summaries}
    checks["historical_current_reconciles"] = all(
        all(int(row[field]) == int(summary_map.get(scope_key(row), {}).get(summary_field, -1))
            for field, summary_field in (("current_finished", "finished"), ("current_with_payload", "with_payload"),
                                         ("current_without_payload", "without_payload"), ("current_actionable_gap", "actionable_gap")))
        and row["calendar_state"] == summary_map[scope_key(row)]["calendar_state"]
        and row["current_mismatch_id_set_hash"] == summary_map[scope_key(row)]["mismatch_id_set_hash"]
        and row["current_primary_outcome"] == primary_reasons(scope_completion_counts(summary_map[scope_key(row)]))
        and json.loads(row["current_completion_reason_counts"]) == summary_map[scope_key(row)]["completion_reason_counts"]
        and json.loads(row["current_section_reason_counts"]) == summary_map[scope_key(row)]["section_reason_counts"]
        for row in historical)
    marker_primary_valid = True
    for row in markers:
        counts = Counter()
        for reason, number in json.loads(row["reason_counts"]).items():
            if not isinstance(number, int) or number <= 0:
                marker_primary_valid = False
            counts[reason.split(":", 1)[0]] += number
        marker_primary_valid &= row["category"] == primary(counts)
    checks["unknown_excluded_from_confirmed"] = bool(marker_primary_valid) and all(
        row["reason"] == "current_payload_present" and csv_bool(row["payload_present"])
        for row in evidence if row["category"] == "confirmed_content")
    source_markers = {row["target_key"]: row for row in load(run / "markers.json")}
    suspect_scopes = {tuple(row["scope"]) for row in load(directory / "candidate-anomalies.json")
                      if row.get("scope") and row["reason"] == "malformed_candidate_capabilities"}
    marker_reconcile = True
    for row in markers:
        source = source_markers.get(row["target_key"])
        if source is None:
            marker_reconcile = False
            continue
        selected, valid, has_matches = marker_scopes(source)
        expected, expected_valid = json_object(source.get("expected_counts_json"))
        capabilities, _ = json_object(source.get("capabilities_json"))
        reasons = Counter()
        if not valid:
            reasons["unknown:malformed_marker_scope_evidence"] += 1
        elif not has_matches:
            reasons["not_applicable:no_matches_family"] += 1
        else:
            for item in selected:
                key = scope_key(item)
                reasons.update(scope_completion_counts(summary_map.get(key), expected.get("matches"), source.get("status")))
                if key in suspect_scopes:
                    reasons["unknown:malformed_candidate_capabilities"] += 1
        if not expected_valid:
            reasons["unknown:malformed_expected_counts"] += 1
        if source.get("status") not in SUCCESS | {"not_available"}:
            reasons["unknown:latest_marker_incomplete"] += 1
        cohorts = sorted({name for selected_scope in marker_work_scopes(source)
                          for name in ("required", "marker_only", "historical_1294")
                          if planned_scopes.get(scope_key(selected_scope), {}).get(name)})
        marker_reconcile &= (json.loads(row["reason_counts"]) == dict(reasons) and
                             row["category"] == primary_reasons(reasons) and
                             json.loads(row["scope_keys"]) == [list(scope_key(item)) for item in selected] and
                             json.loads(row["cohort"]) == cohorts and
                             row["competition_id"] == str(source.get("competition_id") or "") and
                             row["source_season_key"] == (source.get("source_season_key") or "") and
                             row["entity_id"] == str(source.get("entity_id") or "") and
                             row["plan_signature"] == str(capabilities.get("plan_signature", source.get("entity_id")) or "") and
                             row["batch_id"] == str(source["batch_id"]) and row["status"] == source["status"] and
                             row["evidence_sha256"] == sha(canonical(source).encode()))
    checks["marker_completion_and_identity_reconcile"] = bool(marker_reconcile)
    section_scopes = historical_keys | {scope_key(row) for marker in load(run / "markers.json")
                                        for row in marker_work_scopes(marker)}
    expected_sections = {(row["scoped_id"], section) for row in evidence
                         if csv_bool(row["payload_present"]) and scope_key(row) in section_scopes for section in SECTIONS}
    section_ids = [(scoped_id(row), row["section"]) for row in sections]
    checks["six_sections_every_current_payload"] = len(section_ids) == len(set(section_ids)) and set(section_ids) == expected_sections
    checks["section_mismatches_exact_subset"] = section_mismatches == [row for row in sections if row["category"] != "confirmed_content"]
    checks["section_payload_identity"] = all(row["category"] in PRIORITY and row["payload_batch"] ==
                                             evidence_map.get(scoped_id(row), {}).get("payload_batch") for row in sections)
    checks["c1_scoped_reconciliation"] = load(directory / "c1-reconciliation.json") == reconcile_c1(
        load(run / "c1-inputs.json"), evidence, load(run / "snapshots.json"), plan_data["captured_at"])
    checks["all_queries_successful"] = bool(ledger and initial_ledger) and all(
        row["status"] == "success" and row["query_id"] and not row["error_class"] for row in ledger + initial_ledger)
    checks["statement_budget"] = all(0 <= float(row["wall_seconds"]) <= LIMITS["statement_seconds"]
                                      for row in ledger + initial_ledger)
    checks["pass_budget"] = 0 <= timing["pass_wall_seconds"] <= LIMITS["pass_seconds"]
    checks["timing_reconciles"] = (timing["query_count"] == len(ledger) and
                                    timing["max_statement_seconds"] == max((float(row["wall_seconds"]) for row in ledger), default=0))
    frozen = load(run / "chunk-plan.json")
    checks["payload_modes_frozen"] = frozen.get("payload_modes") == payload_modes(
        frozen["scope_chunks"], section_scope_keys(plan_data, load(run / "markers.json")))
    payload_ledger = [row for row in ledger if row["step"] in {"current_payloads", "current_payload_ids"}]
    payload_query_keys = [row["chunk"] for row in payload_ledger]
    permitted_payload_queries = {f"{i}.{j}": frozen["payload_modes"][i]
                                 for i, batches in enumerate(frozen["payload_batches"])
                                 for j in range(len(batches))}
    checks["payload_query_modes"] = (len(payload_query_keys) == len(set(payload_query_keys)) and all(
        permitted_payload_queries.get(row["chunk"]) == row["step"] for row in payload_ledger))
    scope_indices = {scope_key(row): i for i, group in enumerate(frozen["scope_chunks"]) for row in group}
    expected_payload_sets = [set() for group in frozen["scope_chunks"]]
    present_payload_sets = [set() for group in frozen["scope_chunks"]]
    for row in evidence:
        index = scope_indices.get(scope_key(row))
        if index is not None:
            expected_payload_sets[index].add(integer(row["match_id"]))
            if csv_bool(row["payload_present"]):
                present_payload_sets[index].add(integer(row["match_id"]))
    expected_payload_ids = [sorted(values) for values in expected_payload_sets]
    checks["payload_id_chunks_reconcile"] = [
        [mid for batch in group for mid in batch] for group in frozen["payload_batches"]] == expected_payload_ids
    expected_history = {integer(row["match_id"]) for row in evidence if not csv_bool(row["payload_present"]) and
                        (csv_bool(row["finished"]) or csv_bool(row["awarded"]))}
    expected_history.update(integer(row["match_id"]) for row in sections
                            if (flag(evidence_map.get(scoped_id(row), {}).get("finished")) or
                                flag(evidence_map.get(scoped_id(row), {}).get("awarded"))) and
                            (row["category"] == "unknown" or
                             (row["section"] == "matchFacts" and row["category"] != "confirmed_content")))
    checks["history_id_chunks_reconcile"] = [mid for batch in frozen["history_batches"] for mid in batch] == sorted(expected_history)
    payload_query_map = {row["chunk"]: row for row in payload_ledger}
    checks["present_payload_queries_covered"] = all(
        f"{i}.{j}" in payload_query_map
        for i, groups in enumerate(frozen["payload_batches"])
        for j, batch in enumerate(groups)
        if any(mid in present_payload_sets[i] for mid in batch))
    checks["known_collection_steps"] = all(row["step"] in {
        "league_manifest", "match_batch_presence", "current_matches", "match_manifest",
        "current_payloads", "current_payload_ids", "match_evidence"} for row in ledger)
    checks["all_global_buckets_once"] = sorted(int(row["bucket"]) for row in ledger if row["step"] == "match_manifest") == list(range(frozen["bucket_count"]))
    checks["all_scope_chunks_once"] = sorted(int(row["chunk"]) for row in ledger if row["step"] == "league_manifest") == list(range(len(frozen["scope_chunks"])))
    checks["history_chunks_covered"] = sorted(int(row["chunk"]) for row in ledger if row["step"] == "match_evidence") == list(range(len(frozen["history_batches"])))
    checks["planning_queries_covered"] = Counter(row["step"] for row in initial_ledger) == Counter(
        {"snapshots": 1, "markers": 1, "source_gaps": 1, "c1_inputs": 1})
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
            "unknown_marker_targets": sum(row["category"] == "unknown" for row in markers),
            "unknown_id_rows": sum(row["category"] == "unknown" for row in evidence),
            "unknown_section_rows": sum(row["category"] == "unknown" for row in sections),
            "unknown_calendar_scopes": sum(row["calendar_state"] == "unknown" for row in summaries),
            "meaning": "PASS validates complete covered evidence, not universal content confirmation"}


def markdown_table(headers, rows):
    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ")
    return "\n".join(["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"] +
                     ["| " + " | ".join(cell(value) for value in row) + " |" for row in rows])


def build(args):
    run, report = Path(args.run_dir).resolve(), Path(args.report_dir).resolve()
    directory, status = checked_pass(run, args.pass_id)
    if report == run or report == directory or report in directory.parents or run in report.parents:
        raise AuditError("report directory must be separate from the frozen run")
    report.mkdir(parents=True, exist_ok=True)
    validation = validate_report(run, directory)
    dump(report / "validation.json", validation)
    if validation["status"] != "PASS":
        (report / "README.md").write_text(
            "# FotMob #2256 — validation failed\n\n"
            "This invocation produced no valid current report. See validation.json and the frozen local run evidence.\n",
            encoding="utf-8")
        raise AuditError("report validation failed: " + ", ".join(key for key, value in validation["checks"].items() if not value))
    for name in CONTENT_FILES:
        if name not in {"current-id-evidence.csv", "section-evidence.csv"}:
            shutil.copyfile(directory / name, report / name)
    for name in ("snapshots.json", "chunk-plan.json"):
        shutil.copyfile(run / name, report / name)
    shutil.copyfile(directory / "query-ledger.csv", report / "query-ledger.csv")
    shutil.copyfile(run / "query-ledger.csv", report / "planning-query-ledger.csv")
    evidence = read_csv(directory / "current-id-evidence.csv")
    summaries = read_csv(directory / "scope-summary.csv")
    sections = read_csv(directory / "section-evidence.csv")
    markers = read_csv(directory / "marker-results.csv")
    c1 = load(directory / "c1-reconciliation.json")
    timing = load(directory / "timing.json")
    finished = [row for row in evidence if csv_bool(row["finished"])]
    payload = [row for row in evidence if csv_bool(row["payload_present"])]
    literal_gap = [row for row in finished if not csv_bool(row["payload_present"])]
    required = [row for row in finished if csv_bool(row["required"])]
    required_known = [row for row in evidence if csv_bool(row["required"])]
    required_payload = [row for row in required_known if csv_bool(row["payload_present"])]
    required_gap = [row for row in required if not csv_bool(row["payload_present"])]
    global_known = {row["match_id"] for row in evidence}
    global_payload = {row["match_id"] for row in payload}
    competitions = defaultdict(lambda: {"names": set(), "scopes": 0, "known": 0, "finished": 0,
                                        "with_payload": 0, "without_payload": 0, "actionable_gap": 0})
    for row in summaries:
        group = competitions[int(row["competition_id"])]
        group["names"].add(row["catalog_name"])
        group["scopes"] += 1
        for key in ("known", "finished", "with_payload", "without_payload", "actionable_gap"):
            group[key] += int(row[key])
    counts = Counter(row["category"] + ":" + row["reason"] for row in evidence)
    section_counts = Counter((row["section"], row["category"], row["reason"]) for row in sections)
    marker_counts = Counter(row["category"] for row in markers)
    hashes = {"known_scoped": id_set_hash(row["scoped_id"] for row in evidence),
              "payload_scoped": id_set_hash(row["scoped_id"] for row in payload),
              "literal_gap_scoped": id_set_hash(row["scoped_id"] for row in literal_gap),
              "known_global": id_set_hash(global_known), "payload_global": id_set_hash(global_payload),
              "literal_gap_global": id_set_hash(row["match_id"] for row in literal_gap)}
    for name, rows in (("known", required_known), ("payload", required_payload), ("literal_gap", required_gap)):
        hashes["required_" + name + "_scoped"] = id_set_hash(row["scoped_id"] for row in rows)
        hashes["required_" + name + "_global"] = id_set_hash(row["match_id"] for row in rows)
    comparison_path = run / "replay-comparison.json"
    comparison = load(comparison_path) if comparison_path.exists() else None
    if comparison and comparison.get("pass_a") == args.pass_id and comparison.get("status") == "PASS":
        replay_directory, replay_status = checked_pass(run, comparison["pass_b"])
        fresh = compare_content(directory, replay_directory)
        if (fresh["status"] != "PASS" or comparison.get("files") != fresh["files"] or replay_status != status
                or validate_report(run, replay_directory)["status"] != "PASS"):
            raise AuditError("saved replay certificate no longer matches the passes")
        shutil.copyfile(comparison_path, report / "replay-comparison.json")
    else:
        comparison = None
        stale = report / "replay-comparison.json"
        if stale.exists():
            stale.unlink()
    manifest = {"source": "FotMob Bronze", "issue": 2256, "pass_id": args.pass_id,
                "primary_cohort": "#2258 required scopes", "captured_at": load(run / "scope-plan.json")["captured_at"],
                "snapshots": status["snapshots"], "input_hashes": status["input_hashes"],
                "chunk_plan_sha256": status["chunk_plan_sha256"],
                "content_hashes": load(directory / "content-hashes.json"), "id_set_hashes": hashes,
                "local_evidence": {"path": str(directory / "current-id-evidence.csv"),
                                   "sha256": file_sha(directory / "current-id-evidence.csv"), "rows": len(evidence)},
                "local_section_evidence": {"path": str(directory / "section-evidence.csv"),
                                           "sha256": file_sha(directory / "section-evidence.csv"), "rows": len(sections)},
                "plan_path": str(run), "timing": timing,
                "replay_status": comparison["status"] if comparison else "NOT_RUN"}
    dump(report / "run-manifest.json", manifest)
    text = ["# FotMob #2256 — frozen Bronze completion audit", "",
            "Counts below are current-equivalent at the exact Iceberg snapshots in snapshots.json; they are not live source measurements.", "",
            f"Primary #2258 required cohort: **{len(required_known)} known scoped IDs**, **{len(required)} finished**, "
            f"**{sum(csv_bool(row['payload_present']) for row in required)} finished with current payload**, "
            f"**{len(required_gap)} finished without current payload** (literal #1294 rule). "
            f"Actionable applicable gap: **{sum(row['category'] != 'not_applicable' for row in required_gap)}**.", "",
            f"Supplementary union including marker-only/historical scopes: {len(evidence)} known scoped IDs, {len(finished)} finished, "
            f"**{sum(csv_bool(row['payload_present']) for row in finished)} finished with current payload**, "
            f"**{len(literal_gap)} finished without current payload** (literal #1294 rule). "
            f"Actionable applicable gap: **{sum(row['category'] != 'not_applicable' for row in literal_gap)}**.", "",
            "Cohort flags overlap; scope-summary.csv contains each physical scope exactly once.", "",
            f"All current calendar rows: {len(payload)} scoped payload rows. Global distinct known IDs: "
            f"{len(global_known)}; current payload IDs: {len(global_payload)}; finished IDs: "
            f"{len({row['match_id'] for row in finished})}; IDs with any scoped literal gap: "
            f"{len({row['match_id'] for row in literal_gap})}. Shared IDs can be present in one scope and absent in another.", "",
            "## Snapshot and evidence identity", "",
            markdown_table(["Table", "Snapshot", "Made current at"],
                           [(key, row["snapshot_id"], row["made_current_at"]) for key, row in sorted(status["snapshots"].items())]), "",
            markdown_table(["ID set", "SHA256"], sorted(hashes.items())), "",
            f"Full local evidence: `{manifest['local_evidence']['path']}`; SHA256 "
            f"`{manifest['local_evidence']['sha256']}`. This CSV preserves every matching and missing scoped ID, "
            "including global IDs, batch identities, flags and observation times. Preserve the run directory with this report; "
            "the report alone is not a backup of full local evidence.", "",
            f"Full local section evidence: `{manifest['local_section_evidence']['path']}`; SHA256 "
            f"`{manifest['local_section_evidence']['sha256']}`. It contains all six comparisons per selected payload "
            "and must be preserved with the frozen run. Future applicability uses utc_time + 3 hours against "
            f"the single frozen captured_at `{manifest['captured_at']}`.", "",
            "## Supplementary competition breakdown (union of cohorts)", "",
            markdown_table(["ID", "Name", "Scopes", "Known", "Finished", "With payload", "Literal gap", "Actionable gap"],
                           [(cid, "; ".join(sorted(group["names"])), group["scopes"], group["known"], group["finished"],
                             group["with_payload"], group["without_payload"], group["actionable_gap"])
                            for cid, group in sorted(competitions.items())]), "",
            "## Payload reasons", "", markdown_table(["Category:reason", "Scoped rows"], sorted(counts.items())), "",
            "## Latest v2 markers", "", markdown_table(["Primary category", "Targets"], sorted(marker_counts.items())), "",
            "marker-results.csv preserves every target, including interrupted latest rows, all reasons and evidence hashes. "
            "source_unavailable and not_applicable are not content confirmation. Unknown has precedence over every content claim.", "",
            "## Six section comparisons", "",
            markdown_table(["Section", "Category", "Reason", "Scoped observations"],
                           [(*key, number) for key, number in sorted(section_counts.items())]), "",
            "Local section-evidence.csv contains all six comparisons, including agreements, for each current payload in marker or "
            "historical scopes. section-mismatches.csv is its exact non-confirmed subset. Completion and marker totals exclude "
            "ordinary unfinished not-applicable rows; full local evidence retains them. Optional omissions describe the stored "
            "observation only; no permanent capability is inferred.", "",
            "## Late failures and C1 (limited cohort)", "",
            f"Current rows with a later non-success attempt: {sum(csv_bool(row['late_failure']) for row in evidence)}. "
            "A later failure does not remove last-good content.", "",
            f"C1 cached kind=input rows: {c1['cached_input_rows']}; resolved input rows: {c1['input_rows']}; "
            f"distinct scoped input IDs: {c1['distinct_input_ids']}; "
            f"checkpoint/control rows: {c1['control_rows']}; unresolved rows: {c1['unresolved_rows']}. "
            f"C1 ever-ingested no-payload count: {c1['c1_no_payload']}. Current no-payload count within comparable "
            f"C1 IDs: {c1['current_no_payload_comparable']}. Current added since the C1 absence observation: "
            f"{len(c1['late_correction_current_added_ids'])}; ever-ingested but not current: "
            f"{len(c1['ever_ingested_but_not_current_ids'])}. Outside this audit calendar: "
            f"{len(c1['outside_audit_calendar_ids'])}. Exact sets and both snapshot identities are in c1-reconciliation.json. "
            "Equality across different snapshots is not required; C1 does not cover whole history.", "",
            f"C1 first_ft known finished: {c1['first_ft_known_finished']}; finished without ever-ingested payload: "
            f"{c1['finished_without_ever_ingested']}; source-unavailable thresholds met: {c1['source_unavailable_threshold']}. "
            "Deadline-day cohorts, observations, unconfirmed/no-evidence scoped IDs and deterministic set hashes are retained "
            "in c1-reconciliation.json. Thresholds require no success and either three runs or two runs spread over 7,200 seconds.", "",
            "## Historical #1294 baseline — 2026-10-21, not current", "",
            "The old baseline reported 5,305 finished matches WITHOUT details in total; 1,762 of those belong to the "
            "18 competition/season pairs rechecked here. historical-1294-results.csv keeps those keys, competition names, "
            "old signatures and old gap counts separate from current counts, primary outcomes and completion/section reasons. "
            "The original historical CSV bytes and SHA256 are preserved as input evidence.", "",
            "## Validation, timing and limits", "",
            f"Coverage validation: {validation['status']}. Replay: {manifest['replay_status']}. "
            f"Pass wall time: {timing['pass_wall_seconds']} seconds; queries: {timing['query_count']}; "
            f"maximum statement wall time: {timing['max_statement_seconds']} seconds. Limits are 3,600 seconds per pass, "
            "25 seconds per statement, a 30-second HTTP read deadline and a 35-second local process timeout. "
            "Every statement query ID, SQL SHA256 and outcome is saved in the query ledgers.", "",
            f"Unknowns: {validation['unknown_marker_targets']} marker targets, {validation['unknown_id_rows']} scoped IDs, "
            f"{validation['unknown_section_rows']} section observations, {validation['unknown_calendar_scopes']} calendar scopes. "
            "Malformed or unsupported evidence is explicitly unknown. No missing query, timeout or incomplete pass can validate. "
            "A coverage PASS does not assert that unknowns are collected. A single-pass build does not certify replay.", "",
            "Procedure and executable SQL: scripts/research/fotmob_completion_audit/README.md. "
            "No live FotMob access, current views, DDL/DML, S3 replay, DAG changes or scheduled work is used.", ""]
    (report / "README.md").write_text("\n".join(text), encoding="utf-8")
    return validation


def compare_content(directory_a, directory_b):
    files = {name: {"a_sha256": file_sha(directory_a / name), "b_sha256": file_sha(directory_b / name)}
             for name in CONTENT_FILES}
    for row in files.values():
        row["equal"] = row["a_sha256"] == row["b_sha256"]
    return {"status": "PASS" if all(row["equal"] for row in files.values()) else "FAIL", "files": files}


def verify(args):
    run = Path(args.run_dir).resolve()
    if args.pass_a == args.pass_b:
        raise AuditError("replay requires two distinct passes")
    a, a_status = checked_pass(run, args.pass_a)
    b, b_status = checked_pass(run, args.pass_b)
    if a_status != b_status:
        raise AuditError("passes have different frozen identities")
    validations = [validate_report(run, directory) for directory in (a, b)]
    comparison = compare_content(a, b)
    comparison.update(pass_a=args.pass_a, pass_b=args.pass_b, snapshots=a_status["snapshots"],
                      chunk_plan_sha256=a_status["chunk_plan_sha256"], validations=validations)
    if any(value["status"] != "PASS" for value in validations):
        comparison["status"] = "FAIL"
    dump(run / "replay-comparison.json", comparison)
    report = Path(args.report_dir).resolve()
    if report == run or run in report.parents or report in run.parents:
        raise AuditError("report directory must be separate from the frozen run")
    report.mkdir(parents=True, exist_ok=True)
    dump(report / "replay-comparison.json", comparison)
    if comparison["status"] != "PASS":
        dump(report / "validation.json", {"status": "FAIL", "reason": "replay mismatch or invalid coverage"})
        (report / "README.md").write_text(
            "# FotMob #2256 — replay failed\n\n"
            "Two valid identical passes were not established. See replay-comparison.json; no final audit is certified.\n",
            encoding="utf-8")
        raise AuditError("replay comparison failed; see replay-comparison.json")
    return build(argparse.Namespace(run_dir=str(run), pass_id=args.pass_a, report_dir=args.report_dir))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("plan", help="freeze input hashes, snapshots and all scope cohorts")
    command.add_argument("--registry", required=True)
    command.add_argument("--historical", required=True)
    command.add_argument("--run-dir", required=True)
    command.add_argument("--required-scopes", type=int, default=5540)
    command.add_argument("--bucket-count", type=int, default=16)
    command = sub.add_parser("collect", help="collect one calibration or frozen pass")
    command.add_argument("--run-dir", required=True)
    command.add_argument("--pass-id", required=True)
    command.add_argument("--calibrate", action="store_true")
    command = sub.add_parser("build", help="validate and render one complete frozen pass")
    command.add_argument("--run-dir", required=True)
    command.add_argument("--pass-id", required=True)
    command.add_argument("--report-dir", required=True)
    command = sub.add_parser("verify", help="require byte-identical content from two complete passes")
    command.add_argument("--run-dir", required=True)
    command.add_argument("--pass-a", required=True)
    command.add_argument("--pass-b", required=True)
    command.add_argument("--report-dir", required=True)
    args = parser.parse_args(argv)
    try:
        {"plan": plan, "collect": collect, "build": build, "verify": verify}[args.command](args)
    except (AuditError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"audit failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
