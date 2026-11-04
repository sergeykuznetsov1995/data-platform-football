"""Source-faithful match conforming, with per-call state and no storage operations.

The supported source shapes are pinned by the versioned contract. Unsupported
structure fails the complete candidate; optional scalar conversions retain a
NULL reason and raw source value. This module never publishes a partial batch.
"""

from __future__ import annotations

import collections
import copy
import json
import math
import re
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from typing import Any, Sequence

from .contract import Contract


class MaterializationError(ValueError):
    """The candidate cannot be safely materialized."""


@dataclass(frozen=True)
class MaterializedMatches:
    rows: dict[str, list[dict[str, Any]]]
    schema: dict[str, Any]
    diagnostics: tuple[dict[str, Any], ...]
    projections: tuple[dict[str, Any], ...]


def pointer_key(key):
    return str(key).replace("~", "~0").replace("/", "~1")


def normalized(path, key):
    if path == "$.content.playerStats":
        return "{player_id}"
    if path.endswith(".stats[*].stats"):
        return "{metric_label}"
    if path in (
        "$.header.events.homeTeamGoals",
        "$.header.events.awayTeamGoals",
        "$.header.events.homeTeamRedCards",
        "$.header.events.awayTeamRedCards",
    ):
        return "{player_label}"
    return key


def leaves(value, path="$", pointer=""):
    if not isinstance(value, (dict, list)) or not value:
        yield (path, pointer, value)
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from leaves(
                child,
                path + "." + normalized(path, key),
                pointer + "/" + pointer_key(key),
            )
    else:
        for i, child in enumerate(value):
            yield from leaves(child, path + "[*]", pointer + "/" + str(i))


def walk_nodes(value, path="$", pointer=""):
    yield (path, pointer, value)
    if isinstance(value, dict):
        for k, v in value.items():
            yield from walk_nodes(
                v, path + "." + normalized(path, k), pointer + "/" + pointer_key(k)
            )
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from walk_nodes(v, path + "[*]", pointer + "/" + str(i))


def numeric(v):
    if isinstance(v, bool) or v is None:
        return None
    try:
        value = float(v)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _bigint(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        decimal = Decimal(str(value))
        if not decimal.is_finite() or decimal != decimal.to_integral_value():
            return None
        result = int(decimal)
        return result if -(2**63) <= result < 2**63 else None
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return None


def positive(value):
    result = _bigint(value)
    return result if result is not None and result > 0 else None


class _Decoder:
    def __init__(self, contract: Contract, publication_id: str):
        self.schema = copy.deepcopy(contract.schema)
        self.registry = copy.deepcopy(contract.registry)
        self.publication_id = publication_id
        self.rows = {table: [] for table in self.schema["tables"]}
        self.diagnostics = []
        self.aliases = []
        self.projections = []
        self.rejected = []
        self.match_id = None
        self.common = {}

    def row(self, table, node, path, pointer, keys=None):
        fields = (
            self.schema["tables"][table]["fields"]
            if table in self.schema["tables"]
            else {}
        )
        out = {key: None for key in fields}
        out.update(self.common)
        out["source_pointer"] = pointer or "/"
        values = collections.defaultdict(list)
        value_paths = collections.defaultdict(list)
        raw = {}
        reasons = {}
        for source_path, source_pointer, value in leaves(node, path, pointer):
            rule = self.registry.get(source_path)
            if rule and rule["silver_table"] == "silver.fotmob_" + table:
                field = rule["silver_field"]
                values[field].append((source_pointer, value))
                value_paths[field].append(source_path)
                raw[source_pointer] = value
        for field, observations in values.items():
            typ = fields[field]["type"]

            def comparable(value):
                if value is None or isinstance(value, bool):
                    return value
                if typ == "BIGINT":
                    converted = _bigint(value)
                    if converted is not None:
                        return converted
                if typ == "DOUBLE":
                    parsed = numeric(value)
                    if parsed is not None:
                        return parsed
                if typ == "VARCHAR" and (not isinstance(value, (dict, list))):
                    return str(value)
                return value

            unique = []
            for _, value in observations:
                value = comparable(value)
                if not any(
                    (type(value) is type(old) and value == old for old in unique)
                ):
                    unique.append(value)
            if (
                typ.startswith("ARRAY")
                and len(observations) == 1
                and (observations[0][1] is None)
                and ("[*]" not in value_paths[field][0][len(path) :])
            ):
                value = None
            elif (
                typ.startswith("ARRAY")
                and len(observations) == 1
                and isinstance(observations[0][1], list)
            ):
                value = observations[0][1]
            elif typ.startswith("ARRAY"):
                parents = collections.defaultdict(list)
                for ptr, value in observations:
                    parent = (
                        re.sub("/\\d+/(?:name|type|value)$", "", ptr)
                        if re.search("/\\d+/(?:name|type|value)$", ptr)
                        else ptr.rsplit("/", 1)[0]
                    )
                    parents[parent].append(value)
                value = next(iter(parents.values()))
                if any((value != other for other in parents.values())):
                    self.diagnostics.append(
                        dict(
                            kind="array_alias_conflict",
                            table=table,
                            match_id=self.match_id,
                            field=field,
                            pointer=pointer,
                            values=list(parents.values()),
                        )
                    )
            elif typ.startswith("MAP"):
                value = {ptr.rsplit("/", 1)[1]: value for ptr, value in observations}
            elif len(unique) == 1:
                value = unique[0]
            else:
                self.diagnostics.append(
                    dict(
                        kind="scalar_collision",
                        table=table,
                        match_id=self.match_id,
                        field=field,
                        pointer=pointer,
                        observations=observations,
                    )
                )
                reasons[field] = "unresolved_multiple_source_values"
                value = None
            if len(observations) > 1 and len(unique) == 1:
                self.aliases.append(
                    dict(
                        table=table,
                        match_id=self.match_id,
                        field=field,
                        pointers=[p for p, _ in observations],
                    )
                )
            if value is None:
                reasons.setdefault(field, "source_null")
            elif typ == "BIGINT":
                try:
                    converted = _bigint(value)
                    if converted is None:
                        raise ValueError()
                    value = converted
                    if (
                        field
                        in ("source_player_id", "source_event_id", "source_official_id")
                        and value == 0
                    ):
                        value = None
                        reasons[field] = "sentinel_id_zero"
                except (ValueError, TypeError, OverflowError):
                    reasons[field] = "invalid_optional"
                    value = None
            elif typ == "DOUBLE":
                value = numeric(value)
                if value is None:
                    reasons[field] = "invalid_optional"
            elif typ == "VARCHAR" and (not isinstance(value, str)):
                if isinstance(value, (dict, list)):
                    reasons[field] = "invalid_optional"
                    value = None
                else:
                    value = str(value)
            elif typ == "BOOLEAN" and (not isinstance(value, bool)):
                reasons[field] = "invalid_optional"
                value = None
            out[field] = value
        out.update(keys or {})
        if table in (
            "player_match_metrics",
            "player_context_metrics",
            "player_match_ranks",
        ):
            out["player_record_table"] = (
                "match_unavailable_players"
                if "/unavailable/" in out["local_player_record_key"]
                else "match_players"
                if out["local_player_record_key"].startswith("/content/lineup/")
                else "match_player_carriers"
            )
        out["raw_value"] = (keys or {}).get("raw_value", raw)
        out["null_reason"] = reasons or None
        for key, field in fields.items():
            if not field["nullable"] and out.get(key) is None:
                self.diagnostics.append(
                    dict(
                        kind="required_field_unresolved",
                        table=table,
                        match_id=self.match_id,
                        field=key,
                        pointer=pointer,
                    )
                )
        row_index = len(self.rows[table])
        for field, observations in values.items():
            for source_pointer, value in observations:
                self.projections.append(
                    dict(
                        table=table,
                        row_index=row_index,
                        match_id=self.match_id,
                        match_context_key=self.common.get("match_context_key"),
                        publication_id=self.common.get("publication_id"),
                        source_batch_id=self.common.get("source_batch_id"),
                        field=field,
                        source_pointer=source_pointer,
                        source_value=value,
                        row_ref=out,
                    )
                )
        self.rows[table].append(out)
        return out

    def manual_projection(self, out, table, field, ptr, value):
        self.projections.append(
            dict(
                table=table,
                row_index=-1,
                match_id=out["match_id"],
                match_context_key=self.common.get("match_context_key"),
                publication_id=self.common.get("publication_id"),
                source_batch_id=self.common.get("source_batch_id"),
                field=field,
                source_pointer=ptr,
                source_value=value,
                row_ref=out,
            )
        )

    def metric(self, table, node, path, ptr, keys, value=None, text=None):
        out = self.row(table, node, path, ptr, keys)
        if value is not None:
            out["value_number"] = numeric(value)
            if out["value_number"] is None:
                out["value_text"] = str(value)
                self.diagnostics.append(
                    dict(
                        kind="non_numeric_metric",
                        table=table,
                        match_id=self.match_id,
                        pointer=ptr,
                        value=value,
                    )
                )
        if text is not None:
            out["value_text"] = str(text)
        return out

    def player(self, node, path, ptr, team_id=None):
        local = ptr
        keys = {"local_player_record_key": local}
        if team_id is not None:
            keys["team_id"] = team_id
        shallow = {
            k: v
            for k, v in node.items()
            if k not in ("stats", "shotmap", "funFacts", "performance", "rankings")
        }
        table = (
            "match_players"
            if ptr.startswith("/content/lineup/")
            else "match_player_carriers"
        )
        keys["carrier_kind"] = (
            "lineup_" + ptr.split("/")[4]
            if table == "match_players"
            else "playerStats"
            if "/playerStats/" in ptr
            else "playerOfTheMatch"
            if ptr.endswith("/playerOfTheMatch")
            else "topPlayers"
            if "/topPlayers/" in ptr
            else "topScorers"
        )
        out = self.row(table, shallow, path, ptr, keys)
        perf = node.get("performance") or {}
        for key, value in perf.items():
            if isinstance(value, (list, dict)):
                continue
            p = path + ".performance." + key
            q = ptr + "/performance/" + key
            if key == "playerOfTheMatch":
                out["is_player_of_match"] = value
                continue
            table = (
                "player_context_metrics"
                if key in ("seasonRating", "seasonGoals", "seasonAssists")
                else "player_match_metrics"
            )
            contextkey = (
                "source_context"
                if table == "player_context_metrics"
                else "metric_context"
            )
            self.metric(
                table,
                value,
                p,
                q,
                {
                    "local_player_record_key": local,
                    "metric_key": key,
                    contextkey: "lineup.performance",
                },
                value=value,
            )
        for kind in ("events", "substitutionEvents"):
            for i, evidence in enumerate(perf.get(kind) or []):
                q = f"{ptr}/performance/{kind}/{i}"
                self.row(
                    "match_player_observations",
                    evidence,
                    path + ".performance." + kind + "[*]",
                    q,
                    dict(
                        local_player_record_key=local,
                        observation_kind=kind,
                        source_ordinal=i,
                        event_type=evidence.get("type"),
                        minute_raw=evidence.get("time"),
                        reason=evidence.get("reason"),
                        raw_value=evidence,
                    ),
                )
        for metric_key, rank in (node.get("rankings") or {}).items():
            if isinstance(rank, dict):
                self.row(
                    "player_match_ranks",
                    rank,
                    path + ".rankings." + metric_key,
                    ptr + "/rankings/" + metric_key,
                    dict(local_player_record_key=local, metric_key=metric_key),
                )
        for gi, group in enumerate(node.get("stats") or []):
            gp = path + ".stats[*]"
            gptr = ptr + "/stats/" + str(gi)
            for label, item in (group.get("stats") or {}).items():
                ip = gp + ".stats.{metric_label}"
                iptr = gptr + "/stats/" + pointer_key(label)
                keys = dict(
                    local_player_record_key=local,
                    metric_key=item.get("key"),
                    metric_context=gptr,
                    metric_label=label,
                )
                st = item.get("stat") or {}
                if item.get("key") is None:
                    if (
                        label == "Shotmap"
                        and st.get("type") == "boolean"
                        and (type(st.get("value")) is int)
                        and (st["value"] in (0, 1))
                    ):
                        coverage = self.row(
                            "data_coverage",
                            {},
                            "$",
                            iptr,
                            dict(
                                selected_input_version=self.common["source_batch_id"],
                                concrete_source_path=iptr,
                                coverage_scope="player_marker",
                                source_state="present",
                                source_metric_key=None,
                                owner_record_key=local,
                                owner_table="match_players"
                                if ptr.startswith("/content/lineup/")
                                else "match_player_carriers",
                                marker_name="shotmap",
                                source_marker_value=st["value"],
                                marker_available=bool(st["value"]),
                                marker_label=label,
                                marker_stat_type=st["type"],
                            ),
                        )
                        coverage["null_reason"] = {"source_metric_key": "source_null"}
                        self.manual_projection(
                            coverage,
                            "data_coverage",
                            "source_metric_key",
                            iptr + "/key",
                            item["key"],
                        )
                        self.manual_projection(
                            coverage,
                            "data_coverage",
                            "source_marker_value",
                            iptr + "/stat/value",
                            st["value"],
                        )
                        self.manual_projection(
                            coverage,
                            "data_coverage",
                            "marker_stat_type",
                            iptr + "/stat/type",
                            st["type"],
                        )
                    else:
                        self.rejected.append(
                            dict(
                                table="player_match_metrics",
                                match_id=self.match_id,
                                source_pointer=iptr,
                                reason="metric_key_null_unknown_marker",
                                source_value=item,
                            )
                        )
                    continue
                self.metric(
                    "player_match_metrics", item, ip, iptr, keys, value=st.get("value")
                )
                self.row(
                    "metric_definitions",
                    {"key": group.get("key"), "title": group.get("title")},
                    gp,
                    gptr,
                    dict(
                        source_family="playerStats"
                        if "/playerStats/" in ptr
                        else "player_carrier",
                        metric_key=item.get("key"),
                        source_type=st.get("type") or "unknown",
                        context=iptr,
                    ),
                )
        for i, fact in enumerate(node.get("funFacts") or []):
            self.statement(
                fact,
                path + ".funFacts[*]",
                ptr + "/funFacts/" + str(i),
                "funFacts",
                local,
                i,
            )
        return out

    def statement(self, node, path, ptr, source, subject, ordinal):
        out = self.row(
            "match_source_statements",
            node,
            path,
            ptr,
            dict(statement_source=source, subject_key=subject, source_ordinal=ordinal),
        )
        if isinstance(node.get("statValues"), list):
            out.update(
                argument_names=[v.get("name") for v in node["statValues"]],
                argument_types=[v.get("type") for v in node["statValues"]],
                argument_values=[v.get("value") for v in node["statValues"]],
            )
        return out

    def event(self, node, path, ptr):
        key = ptr
        shallow = {
            k: v for k, v in node.items() if k not in ("shotmapEvent", "player", "swap")
        }
        out = self.row("match_events", shallow, path, ptr, dict(event_record_key=key))
        player_obj = node.get("player") or {}
        for field in ("id", "name"):
            if field in player_obj and (
                player_obj[field] is None
                or (field == "id" and player_obj[field] == 0)
                or (
                    field == "name"
                    and isinstance(player_obj[field], str)
                    and (not player_obj[field].strip())
                )
            ):
                q = ptr + "/player/" + field
                v = player_obj[field]
                cov = self.row(
                    "data_coverage",
                    {},
                    "$",
                    q,
                    dict(
                        selected_input_version=self.common["source_batch_id"],
                        concrete_source_path=q,
                        coverage_scope="participant_source_alias",
                        source_state="source_null"
                        if v is None
                        else "source_sentinel_zero"
                        if field == "id" and v == 0
                        else "blank_text",
                        source_marker_value=0 if field == "id" and v == 0 else None,
                        source_text=v if isinstance(v, str) else None,
                    ),
                )
                self.manual_projection(
                    cov,
                    "data_coverage",
                    "source_state"
                    if v is None
                    else "source_marker_value"
                    if field == "id" and v == 0
                    else "source_text",
                    q,
                    v,
                )
        participants = []
        if node.get("type") == "Substitution":
            for i, participant in enumerate(node.get("swap") or []):
                role = ["sub_in", "sub_out"][i] if i < 2 else "unresolved_swap_role"
                participants.append(
                    (participant, path + ".swap[*]", ptr + "/swap/" + str(i), role, i)
                )
        else:
            participant = dict(node.get("player") or {})
            for field in (
                "playerId",
                "firstName",
                "lastName",
                "fullName",
                "nameStr",
                "shortName",
            ):
                if field in node:
                    participant[field] = node[field]
            if positive(participant.get("id") or participant.get("playerId")) or any(
                (
                    str(participant.get(k) or "").strip()
                    for k in ("name", "fullName", "nameStr")
                )
            ):
                role = (
                    "scorer"
                    if node.get("type") == "Goal"
                    else "penalty_taker"
                    if "/penaltyShootoutEvents/" in ptr
                    else "subject"
                )
                subject_node = {
                    k: v
                    for k, v in node.items()
                    if k
                    not in ("assistPlayerId", "assistInput", "shotmapEvent", "swap")
                }
                pr = self.row(
                    "match_event_participants",
                    subject_node,
                    path,
                    ptr,
                    dict(event_record_key=key, role=role, participant_ordinal=0),
                )
                pr["source_player_id"] = positive(
                    participant.get("id") or participant.get("playerId")
                )
                pr["name"] = (
                    participant.get("name")
                    or participant.get("fullName")
                    or participant.get("nameStr")
                )
                pr["first_name"] = participant.get("firstName")
                pr["last_name"] = participant.get("lastName")
            if positive(node.get("assistPlayerId")) or node.get("assistInput"):
                pr = self.row(
                    "match_event_participants",
                    {},
                    "$",
                    ptr + "/assistPlayerId",
                    dict(
                        event_record_key=key,
                        role="assist",
                        participant_ordinal=0,
                        source_player_id=positive(node.get("assistPlayerId")),
                        name=node.get("assistInput"),
                        raw_value={
                            "assistPlayerId": node.get("assistPlayerId"),
                            "assistInput": node.get("assistInput"),
                        },
                    ),
                )
        for participant, p, q, role, ordinal in participants:
            self.row(
                "match_event_participants",
                participant,
                p,
                q,
                dict(event_record_key=key, role=role, participant_ordinal=ordinal),
            )
        return out

    def _materialize(self, record):
        self.match_id = int(record[2])
        data = json.loads(record[10])
        content = data.get("content") or {}
        facts = content.get("matchFacts") or {}
        general = data.get("general") or {}
        header = data.get("header") or {}
        lu = content.get("lineup") or {}
        for path, ptr, value in leaves(data):
            if (
                path not in self.registry
                and path != "$.content.matchFacts.events.events[*].missedPenaltyOutcome"
            ):
                self.rejected.append(
                    dict(
                        match_id=self.match_id,
                        source_pointer=ptr,
                        reason="unknown_source_path",
                        source_value=value,
                    )
                )
        ctx = json.dumps(
            [int(record[0]), record[1], self.match_id],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        self.common = dict(
            match_context_key=ctx,
            competition_id=int(record[0]),
            source_season_key=record[1],
            match_id=self.match_id,
            source_batch_id=record[3],
            publication_id=self.publication_id,
            component_id=record[3],
        )
        root = {k: v for k, v in data.items() if k in ("ongoing", "hasPendingVAR")}
        root["general"] = {
            k: v for k, v in general.items() if k not in ("homeTeam", "awayTeam")
        }
        root["header"] = {"status": header.get("status") or {}}
        info = facts.get("infoBox") or {}
        root["content"] = {
            "hasPlayoff": content.get("hasPlayoff"),
            "matchFacts": {
                k: v for k, v in facts.items() if k in ("matchId", "countryCode")
            },
        }
        root["content"]["matchFacts"]["infoBox"] = {
            k: v for k, v in info.items() if k != "Referee"
        }
        root["content"]["matchFacts"]["matchInsightsConfig"] = (
            facts.get("matchInsightsConfig") or {}
        )
        root["content"]["lineup"] = {
            k: v for k, v in lu.items() if k in ("source", "lineupType", "matchId")
        }
        self.row("matches", root, "$", "")
        for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
            team = general.get(key) or {}
            tid = positive(team.get("id"))
            hteams = [
                x if positive(x.get("id")) == tid else {}
                for x in header.get("teams") or []
            ]
            team_source = {
                "general": {key: team},
                "header": {"teams": hteams},
                "content": {
                    "lineup": {
                        key: {
                            k: v
                            for k, v in (lu.get(key) or {}).items()
                            if k not in ("starters", "subs", "coach", "unavailable")
                        }
                    }
                },
            }
            self.row("match_teams", team_source, "$", "", dict(side=side, team_id=tid))
            for section in ("starters", "subs"):
                for i, item in enumerate((lu.get(key) or {}).get(section) or []):
                    self.player(
                        item,
                        f"$.content.lineup.{key}.{section}[*]",
                        f"/content/lineup/{key}/{section}/{i}",
                        tid,
                    )
            for i, item in enumerate((lu.get(key) or {}).get("unavailable") or []):
                ptr = f"/content/lineup/{key}/unavailable/{i}"
                self.row(
                    "match_unavailable_players",
                    item,
                    f"$.content.lineup.{key}.unavailable[*]",
                    ptr,
                    dict(team_id=tid, local_unavailability_record_key=ptr),
                )
                for mk, mv in (item.get("performance") or {}).items():
                    if mk not in ("seasonGoals", "seasonAssists", "seasonRating"):
                        self.rejected.append(
                            dict(
                                match_id=self.match_id,
                                source_pointer=ptr + "/performance/" + pointer_key(mk),
                                reason="unrecognized_unavailable_metric",
                                source_value=mv,
                            )
                        )
                        continue
                    q = ptr + "/performance/" + pointer_key(mk)
                    out = self.metric(
                        "player_context_metrics",
                        mv,
                        f"$.content.lineup.{key}.unavailable[*].performance." + mk,
                        q,
                        dict(
                            local_player_record_key=ptr,
                            source_context="lineup.unavailable.performance",
                            metric_key=mk,
                        ),
                        value=mv,
                    )
                    self.manual_projection(
                        out, "player_context_metrics", "value_number", q, mv
                    )
            coach = (lu.get(key) or {}).get("coach")
            if isinstance(coach, dict):
                ptr = f"/content/lineup/{key}/coach"
                self.row(
                    "match_officials",
                    coach,
                    f"$.content.lineup.{key}.coach",
                    ptr,
                    dict(role="coach_" + side, local_official_record_key=ptr),
                )
        for pid, item in (content.get("playerStats") or {}).items():
            self.player(
                item, "$.content.playerStats.{player_id}", "/content/playerStats/" + pid
            )
        potm = facts.get("playerOfTheMatch")
        if isinstance(potm, dict) and potm:
            self.player(
                potm,
                "$.content.matchFacts.playerOfTheMatch",
                "/content/matchFacts/playerOfTheMatch",
            )
        for key, items in (facts.get("topPlayers") or {}).items():
            if isinstance(items, list):
                for i, item in enumerate(items):
                    self.player(
                        item,
                        f"$.content.matchFacts.topPlayers.{key}[*]",
                        f"/content/matchFacts/topPlayers/{key}/{i}",
                    )
        for key in ("homePlayer", "awayPlayer"):
            item = (facts.get("topScorers") or {}).get(key)
            if isinstance(item, dict):
                ptr = "/content/matchFacts/topScorers/" + key
                path = "$.content.matchFacts.topScorers." + key
                self.player({k: v for k, v in item.items() if k != "stats"}, path, ptr)
                for mk, mv in (item.get("stats") or {}).items():
                    self.metric(
                        "player_context_metrics",
                        mv,
                        path + ".stats." + mk,
                        ptr + "/stats/" + mk,
                        dict(
                            local_player_record_key=ptr,
                            source_context="topScorers",
                            metric_key=mk,
                            matches_with_rating=item.get("matchesWithRating"),
                            published_rank=item.get("position"),
                        ),
                        value=mv,
                    )
        for period, period_data in (
            (content.get("stats") or {}).get("Periods") or {}
        ).items():
            for gi, group in enumerate(period_data.get("stats") or []):
                gp = f"$.content.stats.Periods.{period}.stats[*]"
                gptr = f"/content/stats/Periods/{period}/stats/{gi}"
                for mi, item in enumerate(group.get("stats") or []):
                    if item.get("type") == "title":
                        iptr = gptr + "/stats/" + str(mi)
                        out = self.row(
                            "metric_definitions",
                            {},
                            "$",
                            iptr,
                            dict(
                                source_family="team_stats_title",
                                metric_key=item.get("key"),
                                source_type="title",
                                context=iptr,
                                group_key=item.get("key"),
                                group_title=item.get("title"),
                            ),
                        )
                        for src, dst in [
                            ("key", "metric_key"),
                            ("key", "group_key"),
                            ("title", "group_title"),
                            ("type", "source_type"),
                        ]:
                            if src in item:
                                self.manual_projection(
                                    out,
                                    "metric_definitions",
                                    dst,
                                    iptr + "/" + src,
                                    item[src],
                                )
                        for i, value in enumerate(item.get("stats") or []):
                            q = iptr + "/stats/" + str(i)
                            cov = self.row(
                                "data_coverage",
                                {},
                                "$",
                                q,
                                dict(
                                    selected_input_version=record[3],
                                    concrete_source_path=q,
                                    coverage_scope="title_slot",
                                    source_state="source_null"
                                    if value is None
                                    else "source_false"
                                    if value is False
                                    else "present",
                                ),
                            )
                            self.manual_projection(
                                cov, "data_coverage", "source_state", q, value
                            )
                            if value is not None:
                                self.rejected.append(
                                    dict(
                                        match_id=self.match_id,
                                        source_pointer=q,
                                        reason="unexpected_nonnull_title_slot",
                                        source_value=value,
                                    )
                                )
                        continue
                    ip = gp + ".stats[*]"
                    iptr = gptr + "/stats/" + str(mi)
                    for si, side in enumerate(("home", "away")):
                        rawstats = item.get("rawStats") or []
                        display = item.get("stats") or []
                        value = (
                            (rawstats[si] or {}).get("value")
                            if si < len(rawstats)
                            else display[si]
                            if si < len(display)
                            else None
                        )
                        values = {
                            k: v
                            for k, v in item.items()
                            if k not in ("stats", "rawStats")
                        }
                        out = self.row(
                            "team_match_metrics",
                            values,
                            ip,
                            iptr,
                            dict(
                                side=side,
                                raw_period=period,
                                metric_key=item.get("key"),
                                value_number=numeric(value),
                                value_text=str(display[si])
                                if si < len(display)
                                else None,
                                denominator=(rawstats[si] or {}).get("total")
                                if si < len(rawstats)
                                else None,
                                raw_value={
                                    "display": display[si]
                                    if si < len(display)
                                    else None,
                                    "rawStats": rawstats[si]
                                    if si < len(rawstats)
                                    else None,
                                },
                            ),
                        )
                        if si < len(display):
                            self.manual_projection(
                                out,
                                "team_match_metrics",
                                "value_text",
                                iptr + "/stats/" + str(si),
                                display[si],
                            )
                        if si < len(rawstats) and "value" in (rawstats[si] or {}):
                            self.manual_projection(
                                out,
                                "team_match_metrics",
                                "value_number",
                                iptr + "/rawStats/" + str(si) + "/value",
                                rawstats[si]["value"],
                            )
                        elif si < len(display):
                            self.manual_projection(
                                out,
                                "team_match_metrics",
                                "value_number",
                                iptr + "/stats/" + str(si),
                                display[si],
                            )
                        if si < len(rawstats) and "total" in (rawstats[si] or {}):
                            self.manual_projection(
                                out,
                                "team_match_metrics",
                                "denominator",
                                iptr + "/rawStats/" + str(si) + "/total",
                                rawstats[si]["total"],
                            )
                    self.row(
                        "metric_definitions",
                        {"key": group.get("key"), "title": group.get("title")},
                        gp,
                        gptr,
                        dict(
                            source_family="team_stats",
                            metric_key=item.get("key"),
                            source_type=item.get("type") or "unknown",
                            context=iptr,
                        ),
                    )
        ev = facts.get("events") or {}
        for section in ("events", "penaltyShootoutEvents"):
            for i, item in enumerate(ev.get(section) or []):
                self.event(
                    item,
                    f"$.content.matchFacts.events.{section}[*]",
                    f"/content/matchFacts/events/{section}/{i}",
                )
        for i, item in enumerate((content.get("shotmap") or {}).get("shots") or []):
            ptr = "/content/shotmap/shots/" + str(i)
            self.row(
                "shots",
                item,
                "$.content.shotmap.shots[*]",
                ptr,
                dict(canonical_coordinate_carrier_path=ptr),
            )

        def objects(value, path="$", ptr=""):
            if isinstance(value, dict):
                yield (path, ptr, value)
                for k, v in value.items():
                    yield from objects(
                        v, path + "." + normalized(path, k), ptr + "/" + pointer_key(k)
                    )
            elif isinstance(value, list):
                for i, v in enumerate(value):
                    yield from objects(v, path + "[*]", ptr + "/" + str(i))

        for path, ptr, obj in objects(data):
            if (
                "id" in obj
                and "x" in obj
                and ("y" in obj)
                and any(
                    (
                        self.registry.get(path + "." + k, {}).get("silver_table")
                        == "silver.fotmob_shot_coordinate_observations"
                        for k in ("x", "y")
                    )
                )
            ):
                self.row(
                    "shot_coordinate_observations",
                    obj,
                    path,
                    ptr,
                    dict(
                        source_shot_id=positive(obj["id"]),
                        carrier_path=ptr,
                        carrier_kind="canonical"
                        if ptr.startswith("/content/shotmap/shots/")
                        else "source_observation",
                        is_canonical=ptr.startswith("/content/shotmap/shots/"),
                    ),
                )
        referee = info.get("Referee")
        if isinstance(referee, dict):
            ptr = "/content/matchFacts/infoBox/Referee"
            path = "$.content.matchFacts.infoBox.Referee"
            self.row(
                "match_officials",
                {k: v for k, v in referee.items() if k != "stats"},
                path,
                ptr,
                dict(role="referee", local_official_record_key=ptr),
            )
            for i, item in enumerate(referee.get("stats") or []):
                self.row(
                    "match_official_metrics",
                    item,
                    path + ".stats[*]",
                    ptr + "/stats/" + str(i),
                    dict(
                        role="referee",
                        local_official_record_key=ptr,
                        source_league_id=positive(referee.get("leagueId")),
                    ),
                )
        for source, container in [("content", content), ("matchFacts", facts)]:
            mo = container.get("momentum") or {}
            main = mo.get("main") or {}
            prefix = (
                "/content"
                + ("/matchFacts" if source == "matchFacts" else "")
                + "/momentum"
            )
            path = "$" + prefix.replace("/", ".")
            for i, point in enumerate(main.get("data") or []):
                self.row(
                    "momentum_points",
                    point,
                    path + ".main.data[*]",
                    prefix + "/main/data/" + str(i),
                    dict(
                        source_series=source,
                        model_slot="main",
                        disambiguating_ordinal=i,
                        model_debug_title=main.get("debugTitle"),
                    ),
                )
        for side, periods in (content.get("attackingZones") or {}).items():
            for period, zones in periods.items():
                for zone, value in zones.items():
                    self.row(
                        "attacking_zones",
                        value,
                        f"$.content.attackingZones.{side}.{period}.{zone}",
                        f"/content/attackingZones/{side}/{period}/{zone}",
                        dict(side=side, raw_period=period, zone=zone),
                    )
        if isinstance(content.get("weather"), dict):
            self.row(
                "match_weather",
                content["weather"],
                "$.content.weather",
                "/content/weather",
            )
        h2h = content.get("h2h") or {}
        if isinstance(h2h.get("summary"), list):
            self.row(
                "match_h2h_summary",
                h2h["summary"],
                "$.content.h2h.summary",
                "/content/h2h/summary",
            )
        for i, item in enumerate(h2h.get("matches") or []):
            self.row(
                "match_related_matches",
                item,
                "$.content.h2h.matches[*]",
                f"/content/h2h/matches/{i}",
                dict(relation_kind="h2h", source_ordinal=i, anchor_team_id=None),
            )
        for si, items in enumerate(facts.get("teamForm") or []):
            for i, item in enumerate(items):
                anchor = next(
                    (
                        positive(item.get(side, {}).get("id"))
                        for side in ("home", "away")
                        if item.get(side, {}).get("isOurTeam")
                    ),
                    None,
                )
                self.row(
                    "match_related_matches",
                    item,
                    "$.content.matchFacts.teamForm[*][*]",
                    f"/content/matchFacts/teamForm/{si}/{i}",
                    dict(
                        relation_kind="teamForm",
                        source_ordinal=i,
                        anchor_team_id=anchor,
                    ),
                )
        for i, item in enumerate(facts.get("QAData") or []):
            self.statement(
                item,
                "$.content.matchFacts.QAData[*]",
                f"/content/matchFacts/QAData/{i}",
                "source_qa",
                "source_assertion",
                i,
            )
        for i, item in enumerate(facts.get("insights") or []):
            self.statement(
                item,
                "$.content.matchFacts.insights[*]",
                f"/content/matchFacts/insights/{i}",
                "insights",
                str(item.get("teamId") or item.get("playerId") or "source_unspecified"),
                i,
            )
        for i, item in enumerate(
            ((facts.get("poll") or {}).get("oddspoll") or {}).get("Facts") or []
        ):
            self.statement(
                item,
                "$.content.matchFacts.poll.oddspoll.Facts[*]",
                f"/content/matchFacts/poll/oddspoll/Facts/{i}",
                "poll_facts",
                "source_template",
                i,
            )
        resources = collections.defaultdict(list)
        for path, ptr, value in leaves(data):
            rule = self.registry.get(path)
            if rule and rule["silver_table"] == "silver.fotmob_match_resource_links":
                owner = ptr.rsplit("/", 1)[0]
                if (
                    self.schema["tables"]["match_resource_links"]["fields"][
                        rule["silver_field"]
                    ]["type"].startswith("ARRAY")
                    and ptr.rsplit("/", 1)[-1].isdigit()
                ):
                    owner = owner.rsplit("/", 1)[0]
                resources[owner].append((path, ptr, value, rule["silver_field"]))
        for owner, items in resources.items():
            urls = [item for item in items if item[3] == "url"]
            groups = (
                [[u] + [item for item in items if item[3] != "url"] for u in urls]
                if urls
                else [items]
            )
            for ordinal, group in enumerate(groups):
                kind = group[0][1].rsplit("/", 1)[-1] if urls else "source_metadata"
                keys = dict(
                    resource_kind=kind,
                    owner_source_pointer=owner,
                    source_ordinal=ordinal,
                )
                out = self.row("match_resource_links", {}, "$", owner, keys)
                for path, ptr, value, field in group:
                    typ = self.schema["tables"]["match_resource_links"]["fields"][
                        field
                    ]["type"]
                    if typ.startswith("ARRAY"):
                        if out[field] is None:
                            out[field] = []
                        out[field].append(value)
                        continue
                    if typ == "BIGINT" and value is not None:
                        value = positive(value)
                    if (
                        typ == "VARCHAR"
                        and value is not None
                        and (not isinstance(value, str))
                    ):
                        value = str(value)
                    if out[field] is not None and out[field] != value:
                        self.diagnostics.append(
                            dict(
                                kind="resource_multi_value",
                                table="match_resource_links",
                                match_id=self.match_id,
                                field=field,
                                pointer=owner,
                                values=[out[field], value],
                            )
                        )
                        out[field] = None
                    else:
                        out[field] = value
                out["raw_value"] = {ptr: value for _, ptr, value, _ in group}
                for path, ptr, value, field in group:
                    self.manual_projection(
                        out, "match_resource_links", field, ptr, value
                    )
        for section in (
            "lineup",
            "stats",
            "playerStats",
            "shotmap",
            "momentum",
            "weather",
            "attackingZones",
            "h2h",
            "table",
        ):
            v = content.get(section)
            state = (
                "missing"
                if section not in content
                else "source_null"
                if v is None
                else "source_false"
                if v is False
                else "empty_array"
                if v == []
                else "empty_object"
                if v == {}
                else "present"
            )
            self.row(
                "data_coverage",
                {},
                "$",
                "/content/" + section,
                dict(
                    selected_input_version=record[3],
                    concrete_source_path="/content/" + section,
                    coverage_scope="section",
                    source_state=state,
                ),
            )
        for field, values, basepath in [
            (
                "declared_filter",
                lu.get("availableFilters"),
                "/content/lineup/availableFilters",
            ),
            (
                "declared_event_type",
                ev.get("eventTypes"),
                "/content/matchFacts/events/eventTypes",
            ),
        ]:
            if isinstance(values, list):
                for i, value in enumerate(values):
                    ptr = basepath + "/" + str(i)
                    out = self.row(
                        "data_coverage",
                        {},
                        "$",
                        ptr,
                        dict(
                            selected_input_version=record[3],
                            concrete_source_path=ptr,
                            coverage_scope="source_declaration",
                            source_state="present",
                            **{field: value},
                        ),
                    )
                    self.manual_projection(out, "data_coverage", field, ptr, value)
        if "ongoing" in ev:
            ptr = "/content/matchFacts/events/ongoing"
            out = self.row(
                "data_coverage",
                {},
                "$",
                ptr,
                dict(
                    selected_input_version=record[3],
                    concrete_source_path=ptr,
                    coverage_scope="source_declaration",
                    source_state="source_null" if ev["ongoing"] is None else "present",
                    events_ongoing=ev["ongoing"],
                ),
            )
            self.manual_projection(
                out, "data_coverage", "events_ongoing", ptr, ev["ongoing"]
            )
        for path, ptr, value in leaves(data):
            rule = self.registry.get(path)
            if (
                rule
                and rule["silver_table"] == "silver.fotmob_data_coverage"
                and (rule["silver_field"] == "source_state")
            ):
                state = (
                    "source_null"
                    if value is None
                    else "source_false"
                    if value is False
                    else "empty_array"
                    if value == []
                    else "empty_object"
                    if value == {}
                    else "present"
                )
                out = next(
                    (
                        r
                        for r in self.rows["data_coverage"]
                        if r["publication_id"] == self.common["publication_id"]
                        and r["match_context_key"] == self.common["match_context_key"]
                        and r["concrete_source_path"] == ptr
                    ),
                    None,
                )
                if out is None:
                    out = self.row(
                        "data_coverage",
                        {},
                        "$",
                        ptr,
                        dict(
                            selected_input_version=record[3],
                            concrete_source_path=ptr,
                            coverage_scope="declared_field_state",
                            source_state=state,
                        ),
                    )
                self.manual_projection(out, "data_coverage", "source_state", ptr, value)
        for path, ptr, value in walk_nodes(data):
            if (
                path
                in (
                    "$.content.momentum.alternateModels",
                    "$.content.matchFacts.momentum.alternateModels",
                )
                or (
                    path.endswith(".cardDescription")
                    and path.startswith("$.header.events.")
                )
                or path == "$.content.matchFacts.events.events[*].missedPenaltyOutcome"
            ):
                if value is None or value == []:
                    state = (
                        "source_null"
                        if value is None
                        else "source_false"
                        if value is False
                        else "empty_array"
                    )
                    out = self.row(
                        "data_coverage",
                        {},
                        "$",
                        ptr,
                        dict(
                            selected_input_version=record[3],
                            concrete_source_path=ptr,
                            coverage_scope="unproven_shape",
                            source_state=state,
                        ),
                    )
                    self.manual_projection(
                        out, "data_coverage", "source_state", ptr, value
                    )
                else:
                    self.rejected.append(
                        dict(
                            match_id=self.match_id,
                            source_pointer=ptr,
                            reason="unproven_nonempty_shape",
                            source_value=value,
                        )
                    )

    def finalize_rows(self):
        owners = {
            (r["match_context_key"], r["local_player_record_key"]): t
            for t in ("match_players", "match_player_carriers")
            for r in self.rows[t]
        }
        owners.update(
            {
                (
                    r["match_context_key"],
                    r["local_unavailability_record_key"],
                ): "match_unavailable_players"
                for r in self.rows["match_unavailable_players"]
            }
        )
        for t in (
            "player_match_metrics",
            "player_context_metrics",
            "player_match_ranks",
        ):
            for r in self.rows[t]:
                r["player_record_table"] = owners[
                    r["match_context_key"], r["local_player_record_key"]
                ]
        groups = collections.defaultdict(list)
        for r in self.rows["team_match_metrics"]:
            groups[
                tuple(
                    (
                        r[k]
                        for k in (
                            "match_context_key",
                            "side",
                            "raw_period",
                            "metric_key",
                        )
                    )
                )
            ].append(r)
        merged = []
        replacement = {}
        for key, group in groups.items():

            def comparable(r):
                return {
                    k: v
                    for k, v in r.items()
                    if k not in ("source_pointer", "source_pointers", "raw_value")
                }

            if not all((comparable(r) == comparable(group[0]) for r in group)):
                raise MaterializationError("conflicting team metric values")
            out = group[0]
            out["source_pointers"] = [r["source_pointer"] for r in group]
            out["raw_value"] = {r["source_pointer"]: r["raw_value"] for r in group}
            for r in group:
                replacement[id(r)] = out
            merged.append(out)
        self.rows["team_match_metrics"] = merged
        for projection in self.projections:
            projection["row_ref"] = replacement.get(
                id(projection["row_ref"]), projection["row_ref"]
            )
        identities = {
            id(r): (t, i) for t, rows in self.rows.items() for i, r in enumerate(rows)
        }
        for projection in self.projections:
            projection["table"], projection["row_index"] = identities[
                id(projection["row_ref"])
            ]


def _identity_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise MaterializationError(f"{label}: expected positive native integer ID")
    try:
        result = int(value)
    except (ValueError, TypeError) as exc:
        raise MaterializationError(f"{label}: invalid native ID") from exc
    if str(result) != str(value) or not 0 < result < 2**63:
        raise MaterializationError(f"{label}: invalid native ID")
    return result


def _validate_record(record: Sequence[str]) -> dict[str, Any]:
    if len(record) != 11:
        raise MaterializationError(
            "match input must contain the 11 frozen Bronze columns"
        )
    competition_id = _identity_int(record[0], "competition_id")
    match_id = _identity_int(record[2], "match_id")
    if (
        not isinstance(record[1], str)
        or not record[1]
        or not isinstance(record[3], str)
        or not record[3]
    ):
        raise MaterializationError("season and selected batch are required")
    try:
        data = json.loads(record[10])
    except (TypeError, json.JSONDecodeError) as exc:
        raise MaterializationError("invalid match payload JSON") from exc
    if not isinstance(data, dict):
        raise MaterializationError("match payload must be an object")
    for section in ("general", "header", "content"):
        if not isinstance(data.get(section), dict):
            raise MaterializationError(
                f"{section}: required object is missing or malformed"
            )
    general, header, content = data["general"], data["header"], data["content"]
    if _identity_int(general.get("matchId"), "general.matchId") != match_id:
        raise MaterializationError("payload match ID disagrees with selected context")
    if (
        _identity_int(general.get("parentLeagueId"), "general.parentLeagueId")
        != competition_id
    ):
        raise MaterializationError(
            "payload competition disagrees with selected context"
        )
    facts = content.get("matchFacts")
    if not isinstance(facts, dict):
        raise MaterializationError("content.matchFacts must be an object")
    for location, candidate in [
        ("header.id", header.get("id")),
        ("matchFacts.matchId", facts.get("matchId")),
    ]:
        if candidate is not None and _identity_int(candidate, location) != match_id:
            raise MaterializationError(f"{location}: mismatching match ID")
    team_ids = []
    for side in ("homeTeam", "awayTeam"):
        team = general.get(side)
        if not isinstance(team, dict):
            raise MaterializationError(f"general.{side}: required team object")
        team_ids.append(_identity_int(team.get("id"), f"general.{side}.id"))
    if team_ids[0] == team_ids[1]:
        raise MaterializationError("home and away identities must differ")
    hteams = header.get("teams")
    if (
        not isinstance(hteams, list)
        or len(hteams) != 2
        or any(not isinstance(t, dict) for t in hteams)
    ):
        raise MaterializationError("header.teams must contain exactly two team objects")
    if [_identity_int(t.get("id"), "header.teams.id") for t in hteams] != team_ids:
        raise MaterializationError("header side identities disagree with general")
    lineup = content.get("lineup")
    if lineup is not None:
        if not isinstance(lineup, dict):
            raise MaterializationError("lineup is not an object or NULL")
        if (
            lineup.get("matchId") is not None
            and _identity_int(lineup["matchId"], "lineup.matchId") != match_id
        ):
            raise MaterializationError("lineup belongs to another match")
        for side, team_id in zip(("homeTeam", "awayTeam"), team_ids):
            team = lineup.get(side)
            if (
                not isinstance(team, dict)
                or _identity_int(team.get("id"), f"lineup.{side}.id") != team_id
            ):
                raise MaterializationError("lineup team belongs to another side")
    return data


def _stable_key(row: dict[str, Any], fields: list[str]) -> tuple[str, ...]:
    return tuple(
        json.dumps(row.get(field), sort_keys=True, ensure_ascii=False, allow_nan=False)
        for field in fields
    )


def _conform_array(value: Any, data_type: str) -> tuple[Any, str | None]:
    """Conform elements explicitly; keep order and distinguish absent arrays."""
    if value is None:
        return None, None
    if not isinstance(value, list):
        return None, "invalid_optional_array"
    result = []
    invalid = False
    for element in value:
        if element is None:
            result.append(None)
        elif data_type == "ARRAY(BIGINT)":
            converted = _bigint(element)
            invalid = invalid or converted is None
            result.append(converted)
        elif data_type == "ARRAY(VARCHAR)":
            if isinstance(element, (str, int, float, bool)):
                if isinstance(element, float) and not math.isfinite(element):
                    result.append(None)
                    invalid = True
                else:
                    result.append(str(element))
            else:
                result.append(None)
                invalid = True
        else:
            result.append(element)
    return result, "invalid_optional_array_element" if invalid else None


def _conform_collections(decoder: _Decoder) -> None:
    # Also cover arrays constructed manually after row(), e.g. statement
    # arguments. Raw input witnesses and projection source_value stay unchanged.
    for table, spec in decoder.schema["tables"].items():
        for row in decoder.rows[table]:
            for field, description in spec["fields"].items():
                if description["type"] not in ("ARRAY(BIGINT)", "ARRAY(VARCHAR)"):
                    continue
                original = row.get(field)
                value, reason = _conform_array(original, description["type"])
                row[field] = value
                if reason:
                    reasons = dict(row.get("null_reason") or {})
                    reasons[field] = reason
                    row["null_reason"] = reasons
                    # The final source array can be a manual projection. Keep it
                    # explicitly even when no registry raw witness was emitted.
                    raw = row.get("raw_value")
                    raw = dict(raw) if isinstance(raw, dict) else {"source": raw}
                    raw["invalid_array:" + field] = original
                    row["raw_value"] = raw


def _validate_rows(decoder: _Decoder) -> None:
    """Require unique declared keys and same-publication unambiguous parents."""
    for table, spec in decoder.schema["tables"].items():
        fields = [f.strip() for f in spec["grain"].split("+")]
        seen = set()
        for row in decoder.rows[table]:
            key = _stable_key(row, fields)
            if key in seen:
                raise MaterializationError(f"{table}: duplicate declared key")
            seen.add(key)
            for field, description in spec["fields"].items():
                if not description["nullable"] and row.get(field) is None:
                    raise MaterializationError(
                        f"{table}.{field}: required value unresolved"
                    )
        for foreign in spec.get("foreign_keys", []):
            parsed = re.fullmatch(r"(\w+)\(([^)]+)\)", foreign["references"])
            if parsed is None:
                raise MaterializationError(f"{table}: malformed foreign-key contract")
            parent, columns = parsed.groups()
            columns = [f.strip() for f in columns.split(",")]
            index = collections.Counter(
                _stable_key(row, columns) for row in decoder.rows[parent]
            )
            if any(count != 1 for count in index.values()):
                raise MaterializationError(f"{table}: ambiguous parent reference")
            if any(
                index[_stable_key(row, foreign["fields"])] != 1
                for row in decoder.rows[table]
            ):
                raise MaterializationError(f"{table}: missing parent reference")
        tagged = spec.get("tagged_player_reference")
        if tagged:
            indexes = {}
            for parent in tagged["allowed_tables"]:
                parent_columns = [
                    "local_unavailability_record_key"
                    if parent == "match_unavailable_players"
                    and field == "local_player_record_key"
                    else field
                    for field in tagged["key_fields"]
                ]
                indexes[parent] = collections.Counter(
                    _stable_key(row, parent_columns) for row in decoder.rows[parent]
                )
            for row in decoder.rows[table]:
                parent = row.get(tagged["table_field"])
                if (
                    parent not in indexes
                    or indexes[parent][_stable_key(row, tagged["key_fields"])] != 1
                ):
                    raise MaterializationError(
                        f"{table}: missing or ambiguous tagged player reference"
                    )


def materialize(
    records: Sequence[Sequence[str]],
    publication_id: str,
    contract: Contract,
) -> MaterializedMatches:
    """Conform an already selected immutable input set; never read or publish data.

    The caller validates committed Bronze manifests before calling this function.
    The complete selected version of each match is rebuilt; no previous children
    are accepted as an input, so an absent child cannot leak from an older version.
    """
    if not isinstance(publication_id, str) or not publication_id.strip():
        raise MaterializationError("publication_id is required")
    if not records:
        raise MaterializationError("a nonempty explicit input set is required")
    decoder = _Decoder(contract, publication_id)
    selected_contexts = set()
    for record in records:
        _validate_record(record)
        if record[7] not in contract.schema["supported_parser_versions"]:
            raise MaterializationError("unsupported Bronze parser version")
        context = (int(record[0]), record[1], int(record[2]))
        if context in selected_contexts:
            raise MaterializationError(
                "multiple selected versions for one match context"
            )
        selected_contexts.add(context)
        try:
            decoder._materialize(record)
        except (KeyError, TypeError, AttributeError, IndexError) as exc:
            raise MaterializationError(
                f"{context}: malformed source structure"
            ) from exc
    if decoder.rejected:
        first = decoder.rejected[0]
        raise MaterializationError(
            f"unsupported source: {first['reason']} at {first['source_pointer']}"
        )
    _conform_collections(decoder)
    decoder.finalize_rows()
    _validate_rows(decoder)
    projections = []
    for item in decoder.projections:
        projection = {key: value for key, value in item.items() if key != "row_ref"}
        projection["projected_value"] = item["row_ref"].get(item["field"])
        projections.append(projection)
    return MaterializedMatches(
        rows=decoder.rows,
        schema=decoder.schema,
        diagnostics=tuple(decoder.diagnostics),
        projections=tuple(projections),
    )
