"""Deterministic repair seeds and Top-5 legacy-quality audit for ESPN v2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml


REPAIR_SEED_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "espn" / "repair_seed.yaml"
)
MEDALLION_COMPETITIONS_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "medallion" / "competitions.yaml"
)
REPAIR_CHECKS = ("date", "duplicate", "final_score", "summary_coverage")
TOP5_COMPETITIONS = MappingProxyType(
    {
        "ENG-Premier League": 700,
        "ESP-La Liga": 740,
        "GER-Bundesliga": 720,
        "ITA-Serie A": 730,
        "FRA-Ligue 1": 710,
    }
)
TOP5_SEASONS = (
    "1617",
    "1718",
    "1819",
    "1920",
    "2021",
    "2122",
    "2223",
    "2324",
    "2425",
    "2526",
)
REPAIR_EXTRACTOR_VERSION = "espn-top5-snapshot-extractor-v1"
SNAPSHOT_TABLES = ("espn_schedule", "espn_lineup", "espn_matchsheet")


class RepairAuditError(ValueError):
    """Repair evidence or the immutable repair seed is malformed."""


def _required(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RepairAuditError(f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, field: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if type(value) is not int or value < minimum:
        raise RepairAuditError(f"{field} must be an integer >= {minimum}")
    return value


def _scope_id(value: object, field: str = "scope_id") -> str:
    raw = _required(value, field)
    parts = raw.split(":")
    if (
        len(parts) != 2
        or not all(part.isdigit() and not part.startswith("0") for part in parts)
        or int(parts[1]) < 1800
    ):
        raise RepairAuditError(f"{field} must be '<espn_id>:<source_year>'")
    return raw


def classify_trust(source_season_year: int) -> str:
    year = _positive_int(source_season_year, "source_season_year")
    return "legacy_untrusted" if year < 2016 else "trusted_candidate"


@dataclass(frozen=True, slots=True)
class RepairSeedScope:
    legacy_league: str
    legacy_season: str
    scope_id: str
    source_season_year: int
    trust_label: str
    registry_promotion_required: bool
    checks: tuple[str, ...]
    reason: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, field: str) -> "RepairSeedScope":
        expected = {
            "legacy_league",
            "legacy_season",
            "scope_id",
            "source_season_year",
            "trust_label",
            "registry_promotion_required",
            "checks",
            "reason",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise RepairAuditError(f"{field} schema mismatch")
        year = _positive_int(value["source_season_year"], f"{field}.source_season_year")
        scope_id = _scope_id(value["scope_id"], f"{field}.scope_id")
        if int(scope_id.split(":", 1)[1]) != year:
            raise RepairAuditError(f"{field} scope/source year mismatch")
        checks = value["checks"]
        if not isinstance(checks, list) or tuple(checks) != REPAIR_CHECKS:
            raise RepairAuditError(f"{field}.checks must be the exact repair gate set")
        trust = _required(value["trust_label"], f"{field}.trust_label")
        if trust != classify_trust(year):
            raise RepairAuditError(f"{field}.trust_label is inconsistent")
        if value["registry_promotion_required"] is not True:
            raise RepairAuditError(f"{field}.registry_promotion_required must be true")
        return cls(
            legacy_league=_required(value["legacy_league"], f"{field}.legacy_league"),
            legacy_season=_required(value["legacy_season"], f"{field}.legacy_season"),
            scope_id=scope_id,
            source_season_year=year,
            trust_label=trust,
            registry_promotion_required=True,
            checks=tuple(checks),
            reason=_required(value["reason"], f"{field}.reason"),
        )


@dataclass(frozen=True, slots=True)
class RepairSeed:
    schema_version: str
    automatic_cutover_min_source_year: int
    pre_2016_trust_label: str
    scopes: tuple[RepairSeedScope, ...]


def load_repair_seed(path: str | Path = REPAIR_SEED_PATH) -> RepairSeed:
    try:
        document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RepairAuditError(f"cannot read repair seed: {exc}") from exc
    if not isinstance(document, Mapping) or set(document) != {
        "schema_version",
        "trust_policy",
        "scopes",
    }:
        raise RepairAuditError("repair seed schema mismatch")
    if document["schema_version"] != "espn-repair-seed-v1":
        raise RepairAuditError("unsupported repair seed schema")
    policy = document["trust_policy"]
    if not isinstance(policy, Mapping) or set(policy) != {
        "automatic_cutover_min_source_year",
        "pre_2016",
    }:
        raise RepairAuditError("repair trust policy schema mismatch")
    minimum = _positive_int(
        policy["automatic_cutover_min_source_year"],
        "automatic_cutover_min_source_year",
    )
    if minimum != 2016 or policy["pre_2016"] != "legacy_untrusted":
        raise RepairAuditError("repair trust boundary must remain 2016")
    raw_scopes = document["scopes"]
    if not isinstance(raw_scopes, list):
        raise RepairAuditError("repair scopes must be a list")
    scopes = tuple(
        RepairSeedScope.from_mapping(item, field=f"scopes[{index}]")
        for index, item in enumerate(raw_scopes)
    )
    identities = [(item.legacy_league, item.legacy_season) for item in scopes]
    if len(identities) != len(set(identities)):
        raise RepairAuditError("repair seed duplicates a legacy scope")
    expected_seed = [
        ("ITA-Serie A", "2021"),
        ("ESP-La Liga", "2021"),
        ("ESP-La Liga", "2324"),
        ("INT-World Cup", "2022"),
        ("FRA-Ligue 1", "1920"),
    ]
    if identities != expected_seed:
        raise RepairAuditError(
            "repair seed must contain the exact approved five scopes"
        )
    return RepairSeed(
        schema_version="espn-repair-seed-v1",
        automatic_cutover_min_source_year=minimum,
        pre_2016_trust_label="legacy_untrusted",
        scopes=scopes,
    )


@dataclass(frozen=True, slots=True)
class Top5Scope:
    scope_id: str
    legacy_league: str
    legacy_season: str
    source_season_year: int
    start_date: date
    end_date: date


def _date_windows(
    path: str | Path = MEDALLION_COMPETITIONS_PATH,
) -> dict[tuple[str, str], tuple[date, date]]:
    try:
        document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RepairAuditError(f"cannot read competition windows: {exc}") from exc
    rows = document.get("competitions") if isinstance(document, Mapping) else None
    if not isinstance(rows, list):
        raise RepairAuditError("competition window catalog schema mismatch")
    result: dict[tuple[str, str], tuple[date, date]] = {}
    for competition in rows:
        if (
            not isinstance(competition, Mapping)
            or competition.get("id") not in TOP5_COMPETITIONS
        ):
            continue
        league = str(competition["id"])
        seasons = competition.get("seasons")
        if not isinstance(seasons, list):
            raise RepairAuditError(f"{league} season windows are missing")
        for raw in seasons:
            if not isinstance(raw, Mapping):
                raise RepairAuditError(f"{league} season window is malformed")
            season = str(raw.get("id"))
            if season not in TOP5_SEASONS:
                continue
            try:
                window = (
                    date.fromisoformat(raw["start"]),
                    date.fromisoformat(raw["end"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise RepairAuditError(
                    f"{league}/{season} date window is malformed"
                ) from exc
            previous = result.setdefault((league, season), window)
            if previous != window:
                raise RepairAuditError(
                    f"{league}/{season} has conflicting date windows"
                )
    expected = {
        (league, season) for league in TOP5_COMPETITIONS for season in TOP5_SEASONS
    }
    if set(result) != expected:
        missing = sorted(expected - set(result))
        raise RepairAuditError(f"Top-5 date window set is incomplete: {missing}")
    return result


def expected_top5_scopes() -> tuple[Top5Scope, ...]:
    windows = _date_windows()
    scopes = []
    for league, espn_id in TOP5_COMPETITIONS.items():
        for season in TOP5_SEASONS:
            source_year = 2000 + int(season[:2])
            start, end = windows[(league, season)]
            scopes.append(
                Top5Scope(
                    scope_id=f"{espn_id}:{source_year}",
                    legacy_league=league,
                    legacy_season=season,
                    source_season_year=source_year,
                    start_date=start,
                    end_date=end,
                )
            )
    return tuple(scopes)


_AUDIT_RECORD_KEYS = {
    "scope_id",
    "legacy_league",
    "legacy_season",
    "source_season_year",
    "trust_label",
    "event_count",
    "observed_min_date",
    "observed_max_date",
    "out_of_window_events",
    "null_schedule_game_ids",
    "duplicate_event_ids",
    "null_lineup_keys",
    "duplicate_lineup_keys",
    "null_matchsheet_keys",
    "duplicate_matchsheet_keys",
    "matchsheet_two_side_failures",
    "final_events",
    "unresolved_final_scores",
    "summary_required_events",
    "summary_covered_events",
}


def _audit_record(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _AUDIT_RECORD_KEYS:
        raise RepairAuditError(f"{field} schema mismatch")
    scope_id = _scope_id(value["scope_id"], f"{field}.scope_id")
    year = _positive_int(value["source_season_year"], f"{field}.source_season_year")
    if int(scope_id.split(":", 1)[1]) != year:
        raise RepairAuditError(f"{field} scope/source year mismatch")
    trust = _required(value["trust_label"], f"{field}.trust_label")
    if trust != classify_trust(year):
        raise RepairAuditError(f"{field}.trust_label is inconsistent")
    counters = {}
    for name in (
        "event_count",
        "out_of_window_events",
        "null_schedule_game_ids",
        "duplicate_event_ids",
        "null_lineup_keys",
        "duplicate_lineup_keys",
        "null_matchsheet_keys",
        "duplicate_matchsheet_keys",
        "matchsheet_two_side_failures",
        "final_events",
        "unresolved_final_scores",
        "summary_required_events",
        "summary_covered_events",
    ):
        counters[name] = _positive_int(value[name], f"{field}.{name}", allow_zero=True)
    if counters["unresolved_final_scores"] > counters["final_events"]:
        raise RepairAuditError(f"{field} unresolved finals exceed final events")
    if counters["summary_covered_events"] > counters["summary_required_events"]:
        raise RepairAuditError(f"{field} Summary coverage exceeds required events")
    raw_date_bounds = (value["observed_min_date"], value["observed_max_date"])
    if counters["event_count"] == 0:
        if raw_date_bounds != (None, None):
            raise RepairAuditError(
                f"{field} empty scope must have null observed date bounds"
            )
        observed_min = observed_max = None
    else:
        try:
            observed_min = date.fromisoformat(value["observed_min_date"])
            observed_max = date.fromisoformat(value["observed_max_date"])
        except (TypeError, ValueError) as exc:
            raise RepairAuditError(f"{field} observed dates are invalid") from exc
        if observed_min > observed_max:
            raise RepairAuditError(f"{field} observed date range is reversed")
    return {
        **dict(value),
        **counters,
        "scope_id": scope_id,
        "legacy_league": _required(value["legacy_league"], f"{field}.legacy_league"),
        "legacy_season": _required(value["legacy_season"], f"{field}.legacy_season"),
        "source_season_year": year,
        "trust_label": trust,
        "observed_min_date": observed_min,
        "observed_max_date": observed_max,
    }


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode()


def _identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or not value.replace("_", "").isalnum():
        raise RepairAuditError(f"{field} is not a safe SQL identifier")
    return value


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _snapshot_ids(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != set(SNAPSHOT_TABLES):
        raise RepairAuditError("snapshot evidence table set is incomplete")
    result = {}
    for table in SNAPSHOT_TABLES:
        snapshot_id = value[table]
        if type(snapshot_id) is not int or snapshot_id <= 0:
            raise RepairAuditError(f"{table} snapshot ID is invalid")
        result[table] = snapshot_id
    return result


def render_top5_audit_sql(
    snapshot_ids: Mapping[str, int],
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> str:
    """Render the one read-only query bound to three exact Iceberg snapshots."""

    snapshots = _snapshot_ids(snapshot_ids)
    catalog = _identifier(catalog, "catalog")
    schema = _identifier(schema, "schema")
    expected = ",\n".join(
        "("
        + ", ".join(
            (
                _sql_literal(scope.scope_id),
                _sql_literal(scope.legacy_league),
                _sql_literal(scope.legacy_season),
                str(scope.source_season_year),
                f"DATE {_sql_literal(scope.start_date.isoformat())}",
                f"DATE {_sql_literal(scope.end_date.isoformat())}",
            )
        )
        + ")"
        for scope in expected_top5_scopes()
    )
    final = (
        "(status LIKE '%FINAL%' OR status IN "
        "('STATUS_FULL_TIME','STATUS_AFTER_EXTRA_TIME','STATUS_AFTER_PENALTIES'))"
    )
    return f"""WITH expected(scope_id, legacy_league, legacy_season, source_season_year, start_date, end_date) AS (
VALUES
{expected}
), schedule AS (
SELECT league, CAST(season AS varchar) AS season, game, TRY_CAST(game_id AS bigint) AS game_id,
       CAST(match_date AS date) AS match_date, status, home_team, away_team,
       home_goals, away_goals
FROM {catalog}.{schema}.espn_schedule FOR VERSION AS OF {snapshots["espn_schedule"]}
), lineup AS (
SELECT league, CAST(season AS varchar) AS season, game, team, player
FROM {catalog}.{schema}.espn_lineup FOR VERSION AS OF {snapshots["espn_lineup"]}
), matchsheet AS (
SELECT league, CAST(season AS varchar) AS season, game, team, is_home
FROM {catalog}.{schema}.espn_matchsheet FOR VERSION AS OF {snapshots["espn_matchsheet"]}
), schedule_stats AS (
SELECT e.scope_id,
       COUNT(s.league) AS event_count,
       CAST(MIN(s.match_date) AS varchar) AS observed_min_date,
       CAST(MAX(s.match_date) AS varchar) AS observed_max_date,
       COUNT_IF(s.league IS NOT NULL AND (s.match_date IS NULL OR s.match_date < e.start_date OR s.match_date > e.end_date)) AS out_of_window_events,
       COUNT_IF(s.league IS NOT NULL AND s.game_id IS NULL) AS null_schedule_game_ids,
       COUNT(s.game_id) - COUNT(DISTINCT s.game_id) AS duplicate_event_ids,
       COUNT_IF(s.league IS NOT NULL AND {final}) AS final_events,
       COUNT_IF(s.league IS NOT NULL AND {final} AND (s.home_goals IS NULL OR s.away_goals IS NULL)) AS unresolved_final_scores
FROM expected e LEFT JOIN schedule s
  ON s.league = e.legacy_league AND s.season = e.legacy_season
GROUP BY e.scope_id
), lineup_stats AS (
SELECT e.scope_id,
       COUNT_IF(l.league IS NOT NULL AND (l.game IS NULL OR l.team IS NULL OR l.player IS NULL)) AS null_lineup_keys,
       COUNT_IF(l.game IS NOT NULL AND l.team IS NOT NULL AND l.player IS NOT NULL)
         - COUNT(DISTINCT IF(l.game IS NOT NULL AND l.team IS NOT NULL AND l.player IS NOT NULL,
             concat(l.game, chr(31), l.team, chr(31), l.player), NULL)) AS duplicate_lineup_keys
FROM expected e LEFT JOIN lineup l
  ON l.league = e.legacy_league AND l.season = e.legacy_season
GROUP BY e.scope_id
), matchsheet_stats AS (
SELECT e.scope_id,
       COUNT_IF(m.league IS NOT NULL AND (m.game IS NULL OR m.team IS NULL)) AS null_matchsheet_keys,
       COUNT_IF(m.game IS NOT NULL AND m.team IS NOT NULL)
         - COUNT(DISTINCT IF(m.game IS NOT NULL AND m.team IS NOT NULL,
             concat(m.game, chr(31), m.team), NULL)) AS duplicate_matchsheet_keys
FROM expected e LEFT JOIN matchsheet m
  ON m.league = e.legacy_league AND m.season = e.legacy_season
GROUP BY e.scope_id
), lineup_games AS (
SELECT l.league, l.season, l.game,
       COUNT(DISTINCT l.team) AS team_count,
       COUNT(DISTINCT IF(l.team IN (s.home_team, s.away_team), l.team, NULL)) AS required_team_count
FROM lineup l JOIN schedule s
  ON s.league = l.league AND s.season = l.season AND s.game = l.game
WHERE l.game IS NOT NULL GROUP BY l.league, l.season, l.game
), matchsheet_games AS (
SELECT league, season, game, COUNT(DISTINCT team) AS team_count,
       COUNT(DISTINCT is_home) AS side_count
FROM matchsheet WHERE game IS NOT NULL GROUP BY league, season, game
), coverage AS (
SELECT e.scope_id,
       COUNT_IF({final} AND (COALESCE(m.team_count, 0) != 2 OR COALESCE(m.side_count, 0) != 2)) AS matchsheet_two_side_failures,
       COUNT_IF({final} AND l.team_count = 2 AND l.required_team_count = 2
         AND m.team_count = 2 AND m.side_count = 2) AS summary_covered_events
FROM expected e LEFT JOIN schedule s
  ON s.league = e.legacy_league AND s.season = e.legacy_season
LEFT JOIN lineup_games l ON l.league = s.league AND l.season = s.season AND l.game = s.game
LEFT JOIN matchsheet_games m ON m.league = s.league AND m.season = s.season AND m.game = s.game
GROUP BY e.scope_id
)
SELECT e.scope_id, e.legacy_league, e.legacy_season, e.source_season_year,
       'trusted_candidate' AS trust_label,
       CAST(s.event_count AS bigint), s.observed_min_date, s.observed_max_date,
       CAST(s.out_of_window_events AS bigint), CAST(s.null_schedule_game_ids AS bigint),
       CAST(s.duplicate_event_ids AS bigint), CAST(l.null_lineup_keys AS bigint),
       CAST(l.duplicate_lineup_keys AS bigint), CAST(m.null_matchsheet_keys AS bigint),
       CAST(m.duplicate_matchsheet_keys AS bigint), CAST(c.matchsheet_two_side_failures AS bigint),
       CAST(s.final_events AS bigint), CAST(s.unresolved_final_scores AS bigint),
       CAST(s.final_events AS bigint) AS summary_required_events,
       CAST(c.summary_covered_events AS bigint)
FROM expected e JOIN schedule_stats s USING (scope_id)
JOIN lineup_stats l USING (scope_id)
JOIN matchsheet_stats m USING (scope_id)
JOIN coverage c USING (scope_id)
ORDER BY e.scope_id"""


def seal_top5_audit_input(
    records: list[Mapping[str, Any]],
    *,
    snapshot_ids: Mapping[str, int],
    as_of: datetime,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> dict[str, Any]:
    if not isinstance(as_of, datetime) or as_of.tzinfo is None:
        raise RepairAuditError("extractor as_of must be timezone-aware")
    snapshots = _snapshot_ids(snapshot_ids)
    sql = render_top5_audit_sql(snapshots, catalog=catalog, schema=schema)
    normalized_records = [dict(record) for record in records]
    return {
        "schema_version": "espn-top5-audit-input-v2",
        "as_of": as_of.astimezone(timezone.utc).isoformat(),
        "snapshot_evidence": {
            "extractor_version": REPAIR_EXTRACTOR_VERSION,
            "catalog": catalog,
            "schema": schema,
            "tables": snapshots,
            "query_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "records_sha256": hashlib.sha256(
                _canonical_bytes(normalized_records)
            ).hexdigest(),
        },
        "records": normalized_records,
    }


_AUDIT_RECORD_ORDER = (
    "scope_id",
    "legacy_league",
    "legacy_season",
    "source_season_year",
    "trust_label",
    "event_count",
    "observed_min_date",
    "observed_max_date",
    "out_of_window_events",
    "null_schedule_game_ids",
    "duplicate_event_ids",
    "null_lineup_keys",
    "duplicate_lineup_keys",
    "null_matchsheet_keys",
    "duplicate_matchsheet_keys",
    "matchsheet_two_side_failures",
    "final_events",
    "unresolved_final_scores",
    "summary_required_events",
    "summary_covered_events",
)


class Top5SnapshotExtractor:
    """Read exact Iceberg main snapshots and compute every repair counter."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def _main_snapshots(self) -> dict[str, int]:
        output = {}
        for table in SNAPSHOT_TABLES:
            rows = self.repository._execute(
                f'SELECT snapshot_id FROM {self.repository.catalog}.{self.repository.schema}."{table}$refs" '
                "WHERE name = 'main' AND type = 'BRANCH'"
            )
            if len(rows) != 1:
                raise RepairAuditError(f"{table} main branch is ambiguous")
            raw = rows[0]
            value = raw.get("snapshot_id") if isinstance(raw, Mapping) else raw[0]
            output[table] = value
        return _snapshot_ids(output)

    def extract(self) -> dict[str, Any]:
        snapshots = self._main_snapshots()
        sql = render_top5_audit_sql(
            snapshots,
            catalog=self.repository.catalog,
            schema=self.repository.schema,
        )
        rows = self.repository._execute(sql)
        records = []
        for raw in rows:
            values = (
                tuple(raw.get(name) for name in _AUDIT_RECORD_ORDER)
                if isinstance(raw, Mapping)
                else tuple(raw)
            )
            if len(values) != len(_AUDIT_RECORD_ORDER):
                raise RepairAuditError("snapshot audit query row is malformed")
            record = dict(zip(_AUDIT_RECORD_ORDER, values))
            for name in _AUDIT_RECORD_ORDER[5:]:
                if name not in {"observed_min_date", "observed_max_date"}:
                    record[name] = int(record[name])
            for name in ("observed_min_date", "observed_max_date"):
                if record[name] is not None:
                    record[name] = str(record[name])
            records.append(record)
        clock_rows = self.repository._execute("SELECT current_timestamp")
        if len(clock_rows) != 1:
            raise RepairAuditError("Trino clock query failed")
        raw_clock = clock_rows[0]
        observed_at = (
            next(iter(raw_clock.values()))
            if isinstance(raw_clock, Mapping)
            else raw_clock[0]
        )
        if isinstance(observed_at, str):
            observed_at = datetime.fromisoformat(observed_at)
        return seal_top5_audit_input(
            records,
            snapshot_ids=snapshots,
            as_of=observed_at,
            catalog=self.repository.catalog,
            schema=self.repository.schema,
        )


def audit_top5(document: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(document, Mapping) or set(document) != {
        "schema_version",
        "as_of",
        "snapshot_evidence",
        "records",
    }:
        raise RepairAuditError("Top-5 audit input schema mismatch")
    if document["schema_version"] != "espn-top5-audit-input-v2":
        raise RepairAuditError("unsupported Top-5 audit input schema")
    try:
        as_of = datetime.fromisoformat(document["as_of"])
    except (TypeError, ValueError) as exc:
        raise RepairAuditError("Top-5 audit as_of is invalid") from exc
    if as_of.tzinfo is None:
        raise RepairAuditError("Top-5 audit as_of must be timezone-aware")
    raw_records = document["records"]
    if not isinstance(raw_records, list):
        raise RepairAuditError("Top-5 audit records must be a list")
    snapshot = document["snapshot_evidence"]
    expected_snapshot_keys = {
        "extractor_version",
        "catalog",
        "schema",
        "tables",
        "query_sha256",
        "records_sha256",
    }
    if not isinstance(snapshot, Mapping) or set(snapshot) != expected_snapshot_keys:
        raise RepairAuditError("Top-5 snapshot evidence schema mismatch")
    if snapshot["extractor_version"] != REPAIR_EXTRACTOR_VERSION:
        raise RepairAuditError("unsupported Top-5 snapshot extractor")
    tables = _snapshot_ids(snapshot["tables"])
    sql = render_top5_audit_sql(
        tables,
        catalog=_identifier(snapshot["catalog"], "snapshot catalog"),
        schema=_identifier(snapshot["schema"], "snapshot schema"),
    )
    if snapshot["query_sha256"] != hashlib.sha256(sql.encode("utf-8")).hexdigest():
        raise RepairAuditError("Top-5 snapshot query hash mismatch")
    if (
        snapshot["records_sha256"]
        != hashlib.sha256(_canonical_bytes(raw_records)).hexdigest()
    ):
        raise RepairAuditError("Top-5 snapshot record hash mismatch")
    records = [
        _audit_record(raw, field=f"records[{index}]")
        for index, raw in enumerate(raw_records)
    ]
    by_scope: dict[str, dict[str, Any]] = {}
    for record in records:
        if record["scope_id"] in by_scope:
            raise RepairAuditError("Top-5 audit duplicates a scope")
        by_scope[record["scope_id"]] = record

    expected = {item.scope_id: item for item in expected_top5_scopes()}
    trusted_ids = {
        scope_id
        for scope_id, row in by_scope.items()
        if row["source_season_year"] >= 2016
    }
    extra = sorted(trusted_ids - set(expected))
    if extra:
        raise RepairAuditError(f"Top-5 audit scope set mismatch; extra={extra}")

    queue = []
    for scope_id, scope in expected.items():
        if scope_id not in by_scope:
            queue.append(
                {
                    "scope_id": scope_id,
                    "legacy_league": scope.legacy_league,
                    "legacy_season": scope.legacy_season,
                    "source_season_year": scope.source_season_year,
                    "trust_label": "trusted_candidate",
                    "registry_promotion_required": True,
                    "reasons": ["missing_scope"],
                }
            )
            continue
        record = by_scope[scope_id]
        identity = (
            record["legacy_league"],
            record["legacy_season"],
            record["source_season_year"],
        )
        if identity != (
            scope.legacy_league,
            scope.legacy_season,
            scope.source_season_year,
        ):
            raise RepairAuditError(f"{scope_id} legacy identity mismatch")
        reasons = []
        if (
            record["event_count"] == 0
            or record["out_of_window_events"] > 0
            or record["observed_min_date"] < scope.start_date
            or record["observed_max_date"] > scope.end_date
        ):
            reasons.append("date")
        if any(
            record[name] > 0
            for name in (
                "null_schedule_game_ids",
                "duplicate_event_ids",
                "null_lineup_keys",
                "duplicate_lineup_keys",
                "null_matchsheet_keys",
                "duplicate_matchsheet_keys",
            )
        ):
            reasons.append("duplicate")
        if record["unresolved_final_scores"] > 0:
            reasons.append("final_score")
        if (
            record["summary_covered_events"] != record["summary_required_events"]
            or record["matchsheet_two_side_failures"] > 0
        ):
            reasons.append("summary_coverage")
        if reasons:
            queue.append(
                {
                    "scope_id": scope_id,
                    "legacy_league": scope.legacy_league,
                    "legacy_season": scope.legacy_season,
                    "source_season_year": scope.source_season_year,
                    "trust_label": "trusted_candidate",
                    "registry_promotion_required": True,
                    "reasons": reasons,
                }
            )

    excluded = [
        {
            "scope_id": record["scope_id"],
            "legacy_league": record["legacy_league"],
            "legacy_season": record["legacy_season"],
            "trust_label": "legacy_untrusted",
            "reason": "pre_2016_not_trusted_for_automatic_cutover",
        }
        for record in records
        if record["source_season_year"] < 2016
    ]
    excluded.sort(key=lambda item: item["scope_id"])
    result = {
        "schema_version": "espn-top5-repair-queue-v1",
        "as_of": as_of.isoformat(),
        "status": "repairs_required" if queue else "passed",
        "audited_scope_count": len(expected),
        "observed_scope_count": len(trusted_ids),
        "queue_count": len(queue),
        "queue": queue,
        "excluded": excluded,
        "trust_boundary": {
            "automatic_cutover_min_source_year": 2016,
            "pre_2016": "legacy_untrusted",
        },
    }
    return {
        **result,
        "result_sha256": hashlib.sha256(_canonical_bytes(result)).hexdigest(),
    }


__all__ = [
    "MEDALLION_COMPETITIONS_PATH",
    "REPAIR_CHECKS",
    "REPAIR_SEED_PATH",
    "TOP5_COMPETITIONS",
    "TOP5_SEASONS",
    "RepairAuditError",
    "RepairSeed",
    "RepairSeedScope",
    "REPAIR_EXTRACTOR_VERSION",
    "SNAPSHOT_TABLES",
    "Top5SnapshotExtractor",
    "Top5Scope",
    "audit_top5",
    "classify_trust",
    "expected_top5_scopes",
    "load_repair_seed",
    "render_top5_audit_sql",
    "seal_top5_audit_input",
]
