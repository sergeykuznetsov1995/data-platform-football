"""
Unit tests for ``dags/sql/silver/whoscored_events_spadl.sql`` — SPADL mapping (E3.9).

Strategy
--------
The SPADL mapping translates 39 distinct WhoScored event ``type`` values into
a 24-value ``action_canonical`` enum. The mapping logic is a monolithic
CASE-WHEN tree driven by ``type`` and ``qualifiers`` (JSON-string array).

We use **DuckDB as an in-memory Trino-bridge**: substitute the iceberg.bronze
table reference with a CTE-style ``bronze_whoscored_events`` table seeded from
fixture rows, and rewrite the few Trino-specific SQL constructs the file uses
(``regexp_like``) into DuckDB equivalents (``regexp_matches``).

This catches behaviour drift in the SPADL CASE tree without requiring a real
Trino + iceberg.bronze backing store.

Coverage
--------
* Pass routing (qualifier-driven): throw_in / goalkick / corner_* / freekick_* /
  cross / pass / empty-qualifier fallback.
* Direct SPADL matches (Foul / TakeOn / Tackle / Interception / Clearance / ...).
* Aerial paired-flag (``_action_source_note='aerial_paired:Aerial'``).
* Shot subtype routing (SavedShot / MissedShots / ShotOnPost / ChanceMissed +
  Penalty / DirectFreekick qualifiers).
* Goalkeeper actions (Save / KeeperSweeper / Smother / Punch / Claim / ...).
* Meta / marker events → 'unknown' + confidence='unmappable'.
* Confidence label cascade (high / medium / low / unmappable).
* Source-backed V2 and stable chronological legacy ``event_id`` contracts.
* Strict-current row parity and removal of the redundant wide dedup window.
* Enum completeness vs ``utils.e3_dq.SPADL_ACTION_ENUM``.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "whoscored_events_spadl.sql"

# Wire dags/ onto sys.path so ``utils.e3_dq`` resolves (matches xref tests pattern).
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# DuckDB bridge — translate Trino-specific SQL to DuckDB-compatible form
# ---------------------------------------------------------------------------


def _translate_trino_to_duckdb(sql: str) -> str:
    """Adapt the Silver SQL for execution on DuckDB.

    Adjustments:
      * ``iceberg.bronze.whoscored_events_current`` → fixture table
        (single-namespace table seeded by the fixture).
      * ``regexp_like(<col>, <pat>)`` → ``regexp_matches(<col>, <pat>)`` —
        DuckDB's idiomatic spelling. Both treat the second argument as a
        POSIX-extended regex, so ``\\s*`` etc. work identically.
      * Trino's ``sha256(to_utf8(json_format(CAST(ROW(...) AS JSON))))``
        tie-breaker → DuckDB's equivalent SHA-256 over ``json_array(...)``.
      * ``LPAD`` / ``TRY_CAST`` / ``ROW_NUMBER OVER`` / ``COALESCE`` —
        already DuckDB-compatible, no rewrite needed.
    """
    # 1. Source table reference.
    sql = sql.replace(
        "iceberg.bronze.whoscored_events_current",
        "bronze_whoscored_events",
    )

    # 2. regexp_like → regexp_matches (both functions take same args).
    sql = re.sub(
        r"\bregexp_like\s*\(",
        "regexp_matches(",
        sql,
        flags=re.IGNORECASE,
    )

    # 3. Trino hashes VARBINARY and therefore needs to_utf8/json_format around
    # the typed ROW. DuckDB's sha256 accepts VARCHAR directly; json_array keeps
    # nulls, value boundaries and the migration-key field order deterministic.
    sql, hash_rewrites = re.subn(
        r"sha256\s*\(\s*to_utf8\s*\(\s*json_format\s*\(\s*"
        r"CAST\s*\(\s*ROW\s*\((?P<fields>[a-z0-9_,\s]+?)\)\s*"
        r"AS\s+JSON\s*\)\s*\)\s*\)\s*\)",
        lambda match: (
            "sha256(CAST(json_array(" + match.group("fields") + ") AS VARCHAR))"
        ),
        sql,
        flags=re.IGNORECASE,
    )
    assert hash_rewrites == 1, "expected one legacy event-id SHA-256 tie-breaker"

    return sql


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


# Bronze schema mirrors the columns the Silver SELECT consumes from
# ``iceberg.bronze.whoscored_events_current`` (per the Silver SELECT contract).
_BRONZE_COLUMNS: List[str] = [
    "game_id",
    "source_event_id",
    "team_event_id",
    "period",
    "minute",
    "second",
    "expanded_minute",
    "type",
    "outcome_type",
    "team_id",
    "player_id",
    "x",
    "y",
    "end_x",
    "end_y",
    "qualifiers",
    "related_team_event_id",
    "related_player_id",
    "team",
    "league",
    "season",
    "_ingested_at",
]

# Must stay byte-for-byte aligned with the canonical natural key in
# scripts.whoscored_v2_object_contract.LEGACY_MIGRATION_KEYS["whoscored_events"].
# The order is part of the JSON-array hash contract, not merely set membership.
_MIGRATION_EVENT_NATURAL_KEY = (
    "league",
    "season",
    "game_id",
    "source_event_id",
    "period",
    "minute",
    "second",
    "expanded_minute",
    "type",
    "outcome_type",
    "team_id",
    "player_id",
    "x",
    "y",
    "end_x",
    "end_y",
    "qualifiers",
    "related_event_id",
    "related_player_id",
    "team",
)


def _row(
    *,
    game_id: int = 100,
    source_event_id: Optional[int] = None,
    team_event_id: Optional[int] = None,
    period: str = "FirstHalf",
    minute: int = 5,
    second: int = 12,
    expanded_minute: int = 5,
    type_: str = "Pass",
    outcome_type: str = "Successful",
    team_id: int = 13,
    player_id: int = 555,
    x: float = 50.0,
    y: float = 50.0,
    end_x: Optional[float] = 60.0,
    end_y: Optional[float] = 50.0,
    qualifiers: Optional[str] = None,
    related_team_event_id: Optional[int] = None,
    related_player_id: Optional[int] = None,
    team: str = "Arsenal",
    league: str = "ENG-Premier League",
    season: str = "2526",
    ingested_at: str = "2026-05-08 12:00:00",
) -> Dict[str, Any]:
    """Build a single bronze fixture row with sensible defaults."""
    return {
        "game_id": game_id,
        "source_event_id": source_event_id,
        "team_event_id": team_event_id,
        "period": period,
        "minute": minute,
        "second": second,
        "expanded_minute": expanded_minute,
        "type": type_,
        "outcome_type": outcome_type,
        "team_id": team_id,
        "player_id": player_id,
        "x": x,
        "y": y,
        "end_x": end_x,
        "end_y": end_y,
        "qualifiers": qualifiers,
        "related_team_event_id": related_team_event_id,
        "related_player_id": related_player_id,
        "team": team,
        "league": league,
        "season": season,
        "_ingested_at": ingested_at,
    }


def _qualifiers_json(types: List[str]) -> str:
    """Render WhoScored qualifiers as a JSON-string array (matches bronze).

    Bronze stores each qualifier as a NESTED object — the sub-type label lives
    in ``type.displayName``, NOT a flat ``type`` string:
        {"type": {"value": N, "displayName": "ThrowIn"}}
    The mapping SQL matches on ``"displayName": "X"`` accordingly. ``value`` is a
    placeholder here (the regex keys off ``displayName``).
    """
    return json.dumps([{"type": {"value": 0, "displayName": t}} for t in types])


@pytest.fixture(scope="session")
def duck_conn():
    """Session-scoped DuckDB connection (faster — keeps connection alive)."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


def _seed_and_run(con, fixture_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop, recreate, seed, and run the translated Silver SQL.

    Returns one dict per output row keyed by the SELECT columns.
    """
    con.execute("DROP TABLE IF EXISTS bronze_whoscored_events")
    con.execute(
        """
        CREATE TABLE bronze_whoscored_events (
            game_id           BIGINT,
            source_event_id    BIGINT,
            team_event_id      BIGINT,
            period            VARCHAR,
            minute            BIGINT,
            second            BIGINT,
            expanded_minute   BIGINT,
            type              VARCHAR,
            outcome_type      VARCHAR,
            team_id           BIGINT,
            player_id         DOUBLE,
            x                 DOUBLE,
            y                 DOUBLE,
            end_x             DOUBLE,
            end_y             DOUBLE,
            qualifiers        VARCHAR,
            related_team_event_id BIGINT,
            related_player_id DOUBLE,
            team              VARCHAR,
            league            VARCHAR,
            season            VARCHAR,
            _ingested_at      TIMESTAMP
        )
        """
    )

    placeholders = ", ".join(["?"] * len(_BRONZE_COLUMNS))
    insert_sql = (
        f"INSERT INTO bronze_whoscored_events "
        f"({', '.join(_BRONZE_COLUMNS)}) VALUES ({placeholders})"
    )
    for row in fixture_rows:
        con.execute(
            insert_sql,
            [row[c] for c in _BRONZE_COLUMNS],
        )

    sql = _translate_trino_to_duckdb(_read_sql())
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Pass routing (the dominant 62.89% bucket — drives overall coverage)
# ---------------------------------------------------------------------------


class TestPassRouting:
    """Pass + qualifier → SPADL action_canonical mapping."""

    def test_throw_in(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=_qualifiers_json(["ThrowIn"])),
            ],
        )
        assert out[0]["action_canonical"] == "throw_in"
        assert out[0]["_action_confidence"] == "medium"

    def test_goalkick(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=_qualifiers_json(["GoalKick"])),
            ],
        )
        assert out[0]["action_canonical"] == "goalkick"

    def test_corner_crossed(self, duck_conn):
        """CornerTaken + Cross → corner_crossed."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(
                    type_="Pass", qualifiers=_qualifiers_json(["CornerTaken", "Cross"])
                ),
            ],
        )
        assert out[0]["action_canonical"] == "corner_crossed"

    def test_corner_short(self, duck_conn):
        """CornerTaken without Cross → corner_short."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=_qualifiers_json(["CornerTaken"])),
            ],
        )
        assert out[0]["action_canonical"] == "corner_short"

    def test_freekick_crossed(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(
                    type_="Pass",
                    qualifiers=_qualifiers_json(["FreekickTaken", "Cross"]),
                ),
            ],
        )
        assert out[0]["action_canonical"] == "freekick_crossed"

    def test_freekick_short(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=_qualifiers_json(["FreekickTaken"])),
            ],
        )
        assert out[0]["action_canonical"] == "freekick_short"

    def test_open_cross(self, duck_conn):
        """Cross qualifier (no other) → cross."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=_qualifiers_json(["Cross"])),
            ],
        )
        assert out[0]["action_canonical"] == "cross"

    def test_default_no_qualifiers(self, duck_conn):
        """Pass with NULL qualifiers → 'pass' fallback (medium confidence)."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=None),
            ],
        )
        assert out[0]["action_canonical"] == "pass"
        assert out[0]["_action_confidence"] == "medium"

    def test_empty_qualifiers_string(self, duck_conn):
        """Pass with empty-string qualifiers → 'pass' fallback."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers=""),
            ],
        )
        assert out[0]["action_canonical"] == "pass"

    def test_empty_array_qualifiers(self, duck_conn):
        """Pass with '[]' literal → 'pass' fallback (per CASE branch)."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Pass", qualifiers="[]"),
            ],
        )
        assert out[0]["action_canonical"] == "pass"

    def test_offside_pass(self, duck_conn):
        """OffsidePass type → 'pass' (still pass, low confidence)."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="OffsidePass", qualifiers=None),
            ],
        )
        assert out[0]["action_canonical"] == "pass"
        assert out[0]["_action_confidence"] == "low"


# ---------------------------------------------------------------------------
# Direct SPADL matches
# ---------------------------------------------------------------------------


class TestDirectMatches:
    """type → action_canonical direct CASE branches (high confidence)."""

    @pytest.mark.parametrize(
        "ws_type, expected",
        [
            ("Foul", "foul"),
            ("TakeOn", "take_on"),
            ("Tackle", "tackle"),
            ("Interception", "interception"),
            ("BlockedPass", "interception"),
            ("Challenge", "tackle"),
            ("Clearance", "clearance"),
        ],
    )
    def test_high_confidence_direct_routes(self, duck_conn, ws_type, expected):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type)])
        assert out[0]["action_canonical"] == expected
        assert out[0]["_action_confidence"] == "high"


# ---------------------------------------------------------------------------
# Proprietary supplement: BallRecovery
# ---------------------------------------------------------------------------


class TestBallRecoveryProprietary:
    """BallRecovery is the single non-SPADL proprietary supplement."""

    def test_ball_recovery_is_low_confidence(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(type_="BallRecovery")])
        assert out[0]["action_canonical"] == "ball_recovery"
        assert out[0]["_action_confidence"] == "low"


# ---------------------------------------------------------------------------
# Aerial paired duels
# ---------------------------------------------------------------------------


class TestAerialPaired:
    """Aerial events get tackle + an aerial_paired marker in the audit note."""

    def test_aerial_routes_to_tackle(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(type_="Aerial")])
        assert out[0]["action_canonical"] == "tackle"

    def test_aerial_audit_note_has_aerial_paired_marker(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(type_="Aerial")])
        assert "aerial_paired" in (out[0]["_action_source_note"] or "")

    def test_aerial_confidence_medium(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(type_="Aerial")])
        assert out[0]["_action_confidence"] == "medium"


# ---------------------------------------------------------------------------
# Shot subtype routing
# ---------------------------------------------------------------------------


class TestShotSubtypes:
    """Shot variants resolve to shot / shot_freekick / shot_penalty by qualifier."""

    @pytest.mark.parametrize(
        "ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"]
    )
    def test_shot_default(self, duck_conn, ws_type):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type, qualifiers=None)])
        assert out[0]["action_canonical"] == "shot"

    @pytest.mark.parametrize(
        "ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"]
    )
    def test_shot_penalty(self, duck_conn, ws_type):
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_=ws_type, qualifiers=_qualifiers_json(["Penalty"])),
            ],
        )
        assert out[0]["action_canonical"] == "shot_penalty"

    @pytest.mark.parametrize(
        "ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"]
    )
    def test_shot_freekick(self, duck_conn, ws_type):
        """DirectFreekick qualifier on shot → shot_freekick.

        Free-kick *shots* carry `DirectFreekick`; `FreekickTaken` only tags the
        pass set-piece and never appears on a shot event.
        """
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_=ws_type, qualifiers=_qualifiers_json(["DirectFreekick"])),
            ],
        )
        assert out[0]["action_canonical"] == "shot_freekick"

    def test_chance_missed_routes_to_shot(self, duck_conn):
        """ChanceMissed → shot (low confidence per R3.D2 #38)."""
        out = _seed_and_run(duck_conn, [_row(type_="ChanceMissed")])
        assert out[0]["action_canonical"] == "shot"
        assert out[0]["_action_confidence"] == "low"


# ---------------------------------------------------------------------------
# Own-goal routing (#572)
# ---------------------------------------------------------------------------


class TestOwnGoal:
    """Goal + OwnGoal qualifier routes to 'own_goal' (NOT the shot family), so
    the scorer is not credited with a shot. Plain Goals stay 'shot' (#462)."""

    def test_own_goal_routes_to_own_goal(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Goal", qualifiers=_qualifiers_json(["OwnGoal"])),
            ],
        )
        assert out[0]["action_canonical"] == "own_goal"

    def test_plain_goal_still_routes_to_shot(self, duck_conn):
        """Guard against over-matching: a Goal without the OwnGoal qualifier
        must still land in the shot family (#462 behaviour preserved)."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Goal", qualifiers=None),
            ],
        )
        assert out[0]["action_canonical"] == "shot"

    def test_own_goal_in_enum(self, duck_conn):
        """'own_goal' must be a registered SPADL_ACTION_ENUM value or the
        DQ enum-violation guard would hard-fail on it."""
        from utils.e3_dq import SPADL_ACTION_ENUM

        assert "own_goal" in SPADL_ACTION_ENUM
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="Goal", qualifiers=_qualifiers_json(["OwnGoal"])),
            ],
        )
        assert out[0]["action_canonical"] in set(SPADL_ACTION_ENUM)


# ---------------------------------------------------------------------------
# Goalkeeper actions
# ---------------------------------------------------------------------------


class TestGoalkeeperActions:
    @pytest.mark.parametrize(
        "ws_type, expected",
        [
            ("Save", "keeper_save"),
            ("KeeperSweeper", "keeper_save"),
            ("Smother", "keeper_save"),
            ("PenaltyFaced", "keeper_save"),
            ("KeeperPickup", "keeper_pick_up"),
            ("Punch", "keeper_punch"),
            ("Claim", "keeper_claim"),
            ("CrossNotClaimed", "keeper_claim"),
        ],
    )
    def test_keeper_action_routing(self, duck_conn, ws_type, expected):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type)])
        assert out[0]["action_canonical"] == expected


# ---------------------------------------------------------------------------
# BallTouch / Dispossessed / Error / ShieldBallOpp / GoodSkill
# ---------------------------------------------------------------------------


class TestBallTouchAndDribbleVariants:
    """Per R3.D2 (file header L77-89, L294-300)."""

    def test_ball_touch_unsuccessful(self, duck_conn):
        """BallTouch with outcome_type='Unsuccessful' → bad_touch."""
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="BallTouch", outcome_type="Unsuccessful"),
            ],
        )
        assert out[0]["action_canonical"] == "bad_touch"
        assert out[0]["outcome_success"] is False

    def test_ball_touch_successful(self, duck_conn):
        """BallTouch with outcome_type='Successful' → bad_touch (parity-preserving;
        R3.D2 originally proposed dropping but kept for row-count parity).
        """
        out = _seed_and_run(
            duck_conn,
            [
                _row(type_="BallTouch", outcome_type="Successful"),
            ],
        )
        assert out[0]["action_canonical"] == "bad_touch"
        assert out[0]["outcome_success"] is True

    @pytest.mark.parametrize(
        "ws_type, expected",
        [
            ("Dispossessed", "bad_touch"),
            ("Error", "bad_touch"),
            ("ShieldBallOpp", "dribble"),
            ("GoodSkill", "dribble"),
        ],
    )
    def test_dribble_and_bad_touch_routes(self, duck_conn, ws_type, expected):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type)])
        assert out[0]["action_canonical"] == expected


# ---------------------------------------------------------------------------
# Meta / marker events → unknown
# ---------------------------------------------------------------------------


class TestMetaEventsBecomeUnknown:
    """All 10 meta/marker types collapse to 'unknown' with confidence='unmappable'.

    Goal is NOT here — it routes to the shot family (see TestShotSubtypes / #462).
    """

    META_TYPES = [
        "Card",
        "SubstitutionOn",
        "SubstitutionOff",
        "Start",
        "End",
        "FormationSet",
        "FormationChange",
        "CornerAwarded",
        "OffsideProvoked",
        "OffsideGiven",
    ]

    @pytest.mark.parametrize("ws_type", META_TYPES)
    def test_meta_routes_to_unknown(self, duck_conn, ws_type):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type)])
        assert out[0]["action_canonical"] == "unknown"
        assert out[0]["_action_confidence"] == "unmappable"

    def test_unknown_genuinely_unknown_type(self, duck_conn):
        """Defensive: an unforeseen WhoScored type also collapses to 'unknown'."""
        out = _seed_and_run(duck_conn, [_row(type_="MysteryFutureEventType")])
        assert out[0]["action_canonical"] == "unknown"
        assert out[0]["_action_confidence"] == "unmappable"


# ---------------------------------------------------------------------------
# Schema-version literal pin (R0.4)
# ---------------------------------------------------------------------------


class TestSchemaVersionLiterals:
    def test_action_source_literal(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row()])
        assert out[0]["action_source"] == "whoscored_spadl_proprietary_v1"

    def test_action_version_literal(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row()])
        assert out[0]["action_version"] == "v1"


# ---------------------------------------------------------------------------
# Synthetic event_id uniqueness
# ---------------------------------------------------------------------------


class TestStableEventId:
    """V2 uses source IDs; migrated legacy rows retain sequence IDs."""

    def test_source_event_id_is_preferred(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(
                    game_id=200,
                    source_event_id=98765,
                    team_event_id=65,
                    related_team_event_id=64,
                ),
            ],
        )
        assert out[0]["event_id"] == "ws:200:98765"
        assert out[0]["source_event_id_raw"] == "98765"
        assert out[0]["team_event_id_raw"] == "65"
        assert out[0]["related_team_event_id_raw"] == "64"

    def test_legacy_event_id_format(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(game_id=200, minute=1, second=0, type_="Pass"),
            ],
        )
        assert out[0]["event_id"] == "200_00001"

    def test_event_id_unique_per_game(self, duck_conn):
        rows = [
            _row(game_id=300, minute=1, second=0, type_="Pass"),
            _row(game_id=300, minute=2, second=0, type_="Pass"),
            _row(game_id=300, minute=3, second=0, type_="Foul"),
            _row(game_id=300, minute=4, second=0, type_="Tackle"),
        ]
        out = _seed_and_run(duck_conn, rows)
        ids = [r["event_id"] for r in out]
        assert len(ids) == len(set(ids)), f"duplicate event_ids: {ids}"
        assert all(eid.startswith("300_") for eid in ids)

    def test_event_id_distinct_across_games(self, duck_conn):
        rows = [
            _row(game_id=301, minute=1, second=0, type_="Pass"),
            _row(game_id=302, minute=1, second=0, type_="Pass"),
        ]
        out = _seed_and_run(duck_conn, rows)
        # Same minute/second but different games have different id prefixes.
        prefixes = {r["event_id"].split("_")[0] for r in out}
        assert prefixes == {"301", "302"}

    def test_legacy_id_is_stable_across_ingest_timestamp(self, duck_conn):
        older = _seed_and_run(
            duck_conn,
            [
                _row(game_id=303, minute=12, ingested_at="2026-05-01 10:00:00"),
            ],
        )[0]["event_id"]
        newer = _seed_and_run(
            duck_conn,
            [
                _row(game_id=303, minute=12, ingested_at="2026-07-11 10:00:00"),
            ],
        )[0]["event_id"]
        assert older == newer

    def test_match_id_is_varchar_game_id(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(game_id=400)])
        assert out[0]["match_id"] == "400"


# ---------------------------------------------------------------------------
# #736: raw passthrough columns for Gold fct_match_timeline one-hop
# ---------------------------------------------------------------------------


class TestRawPassthroughColumns736:
    """#736: silver projects raw minute/second/related_player_id/team so Gold
    fct_match_timeline reads the WhoScored fallback from silver (one-hop) instead
    of bronze. Values are byte-for-byte the bronze columns (ids double-cast)."""

    def test_minute_and_second_passthrough(self, duck_conn):
        out = _seed_and_run(
            duck_conn,
            [
                _row(game_id=500, minute=47, second=12, type_="Pass"),
            ],
        )
        assert out[0]["minute"] == 47
        assert out[0]["second"] == 12

    def test_team_name_raw_passthrough(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(team="Arsenal")])
        assert out[0]["team_name_raw"] == "Arsenal"

    def test_related_player_id_raw_double_cast(self, duck_conn):
        # bronze related_player_id is DOUBLE; a naive CAST AS varchar yields
        # scientific notation ('9.5408E4'). The BIGINT round-trip keeps digits.
        out = _seed_and_run(duck_conn, [_row(related_player_id=95408)])
        assert out[0]["related_player_id_raw"] == "95408"

    def test_related_player_id_raw_null_stays_null(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(related_player_id=None)])
        assert out[0]["related_player_id_raw"] is None


# ---------------------------------------------------------------------------
# Strict-current row parity
# ---------------------------------------------------------------------------


class TestStrictCurrentParity:
    """Silver is a one-for-one projection of the manifest-current view."""

    def test_does_not_hide_upstream_contract_violation(self, duck_conn):
        # The migration/current-view contract prevents this duplicate in
        # production. If it ever regresses, Silver must preserve both rows so
        # it remains observable instead of silently deduplicating 28M rows.
        rows = [
            _row(
                game_id=500,
                minute=1,
                second=0,
                type_="Pass",
                qualifiers=_qualifiers_json(["Cross"]),
                ingested_at="2026-05-01 10:00:00",
            ),
            _row(
                game_id=500,
                minute=1,
                second=0,
                type_="Pass",
                qualifiers=_qualifiers_json(["Cross"]),
                ingested_at="2026-05-08 10:00:00",  # newer
            ),
        ]
        out = _seed_and_run(duck_conn, rows)
        assert len(out) == len(rows)
        assert {r["action_canonical"] for r in out} == {"cross"}
        assert len({r["event_id"] for r in out}) == len(rows)


# ---------------------------------------------------------------------------
# Confidence cascade
# ---------------------------------------------------------------------------


class TestConfidenceCascade:
    """The confidence label distribution per file header L342-364."""

    @pytest.mark.parametrize(
        "ws_type, conf",
        [
            ("Foul", "high"),
            ("TakeOn", "high"),
            ("Tackle", "high"),
            ("Interception", "high"),
            ("BlockedPass", "high"),
            ("Challenge", "high"),
            ("Clearance", "high"),
            ("Save", "high"),
            ("KeeperPickup", "high"),
            ("Claim", "high"),
            ("Punch", "high"),
            ("Smother", "high"),
            ("Pass", "medium"),
            ("Aerial", "medium"),
            ("BallTouch", "medium"),
            ("Dispossessed", "medium"),
            ("SavedShot", "medium"),
            ("MissedShots", "medium"),
            ("ShotOnPost", "medium"),
            ("Goal", "medium"),
            ("KeeperSweeper", "medium"),
            ("GoodSkill", "medium"),
            ("CrossNotClaimed", "medium"),
            ("BallRecovery", "low"),
            ("OffsidePass", "low"),
            ("Error", "low"),
            ("ShieldBallOpp", "low"),
            ("PenaltyFaced", "low"),
            ("ChanceMissed", "low"),
            # unmappable — sample
            ("Card", "unmappable"),
            ("Start", "unmappable"),
            ("End", "unmappable"),
            ("FormationChange", "unmappable"),
        ],
    )
    def test_confidence_for_type(self, duck_conn, ws_type, conf):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type)])
        assert out[0]["_action_confidence"] == conf, (
            f"confidence mismatch for type={ws_type}: expected {conf}, "
            f"got {out[0]['_action_confidence']}"
        )


# ---------------------------------------------------------------------------
# Outcome flag
# ---------------------------------------------------------------------------


class TestOutcomeSuccess:
    def test_successful_outcome_is_true(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(outcome_type="Successful")])
        assert out[0]["outcome_success"] is True

    def test_unsuccessful_outcome_is_false(self, duck_conn):
        out = _seed_and_run(duck_conn, [_row(outcome_type="Unsuccessful")])
        assert out[0]["outcome_success"] is False


# ---------------------------------------------------------------------------
# Cross-file enum completeness
# ---------------------------------------------------------------------------


class TestEnumCompleteness:
    """SPADL_ACTION_ENUM in utils.e3_dq must cover every value the SQL emits."""

    def test_action_canonical_enum_size_is_25(self):
        from utils.e3_dq import SPADL_ACTION_ENUM

        assert len(SPADL_ACTION_ENUM) == 25

    def test_all_observed_routes_in_enum(self, duck_conn):
        """Run every documented WhoScored type once and assert the produced
        action_canonical lands inside the enum (no silent drift).
        """
        from utils.e3_dq import SPADL_ACTION_ENUM

        allowed = set(SPADL_ACTION_ENUM)

        # Sample one row per type — the 39 documented WhoScored types.
        documented_types = [
            "Pass",
            "BallRecovery",
            "BallTouch",
            "Aerial",
            "Clearance",
            "Foul",
            "TakeOn",
            "Tackle",
            "CornerAwarded",
            "Dispossessed",
            "Interception",
            "BlockedPass",
            "Challenge",
            "SavedShot",
            "Save",
            "KeeperPickup",
            "MissedShots",
            "SubstitutionOff",
            "SubstitutionOn",
            "End",
            "Card",
            "Start",
            "OffsideProvoked",
            "OffsideGiven",
            "OffsidePass",
            "FormationChange",
            "Goal",
            "FormationSet",
            "Claim",
            "Error",
            "ShieldBallOpp",
            "KeeperSweeper",
            "Punch",
            "ShotOnPost",
            "Smother",
            "PenaltyFaced",
            "GoodSkill",
            "ChanceMissed",
            "CrossNotClaimed",
        ]
        rows = [
            _row(game_id=900 + i, minute=i + 1, second=0, type_=t)
            for i, t in enumerate(documented_types)
        ]
        out = _seed_and_run(duck_conn, rows)

        observed = {r["action_canonical"] for r in out}
        leaked = observed - allowed
        assert not leaked, (
            f"action_canonical values not in SPADL_ACTION_ENUM: {sorted(leaked)}"
        )

    def test_qualifier_branches_also_in_enum(self, duck_conn):
        """Pass+qualifier branches and shot-subtype branches all in enum too."""
        from utils.e3_dq import SPADL_ACTION_ENUM

        allowed = set(SPADL_ACTION_ENUM)

        rows = [
            _row(
                game_id=1000,
                minute=1,
                type_="Pass",
                qualifiers=_qualifiers_json(["ThrowIn"]),
            ),
            _row(
                game_id=1000,
                minute=2,
                type_="Pass",
                qualifiers=_qualifiers_json(["GoalKick"]),
            ),
            _row(
                game_id=1000,
                minute=3,
                type_="Pass",
                qualifiers=_qualifiers_json(["CornerTaken", "Cross"]),
            ),
            _row(
                game_id=1000,
                minute=4,
                type_="Pass",
                qualifiers=_qualifiers_json(["CornerTaken"]),
            ),
            _row(
                game_id=1000,
                minute=5,
                type_="Pass",
                qualifiers=_qualifiers_json(["FreekickTaken", "Cross"]),
            ),
            _row(
                game_id=1000,
                minute=6,
                type_="Pass",
                qualifiers=_qualifiers_json(["FreekickTaken"]),
            ),
            _row(
                game_id=1000,
                minute=7,
                type_="Pass",
                qualifiers=_qualifiers_json(["Cross"]),
            ),
            _row(
                game_id=1000,
                minute=8,
                type_="SavedShot",
                qualifiers=_qualifiers_json(["Penalty"]),
            ),
            _row(
                game_id=1000,
                minute=9,
                type_="SavedShot",
                qualifiers=_qualifiers_json(["DirectFreekick"]),
            ),
        ]
        out = _seed_and_run(duck_conn, rows)
        observed = {r["action_canonical"] for r in out}
        leaked = observed - allowed
        assert not leaked, f"qualifier branch leaked outside enum: {sorted(leaked)}"


# ---------------------------------------------------------------------------
# Memory-safe plan shape and order independence
# ---------------------------------------------------------------------------


class TestMemorySafeProjection:
    """Only the compatibility-critical, per-match sequence window may remain."""

    @staticmethod
    def _executable_sql() -> str:
        sql = re.sub(r"--[^\n]*", "", _read_sql())
        return re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

    def test_removes_wide_dedup_and_keeps_one_bounded_window(self):
        sql = self._executable_sql()
        windows = re.findall(r"\bROW_NUMBER\s*\(", sql, re.IGNORECASE)
        assert len(windows) == 1
        assert not re.search(
            r"\b(?:GROUP\s+BY|DISTINCT|JOIN|WHERE)\b",
            sql,
            re.IGNORECASE,
        )
        window = re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\((.*?)\)\s*AS\s+event_seq",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        assert window
        body = window.group(1)
        partition, order = re.split(
            r"\bORDER\s+BY\b",
            body,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        assert re.search(
            r"PARTITION\s+BY\s+league\s*,\s*season\s*,\s*game_id\b",
            partition,
            re.IGNORECASE,
        )
        # The OOMing predecessor put the full natural key — including the
        # qualifiers JSON payload — in the window partition key.
        assert "qualifiers" not in partition.lower()
        assert order.lower().count("sha256") == 1

    def test_tie_breaker_hashes_complete_migration_natural_key(self):
        sql = self._executable_sql()
        hashed_row = re.search(
            r"sha256\s*\(\s*to_utf8\s*\(\s*json_format\s*\(\s*"
            r"CAST\s*\(\s*ROW\s*\((?P<fields>[a-z0-9_,\s]+?)\)\s*"
            r"AS\s+JSON\s*\)\s*\)\s*\)\s*\)",
            sql,
            flags=re.IGNORECASE,
        )
        assert hashed_row, "legacy sequence must end in a typed SHA-256 key"
        hash_fields = tuple(
            field.strip() for field in hashed_row.group("fields").split(",")
        )
        # The reversible Bronze migration honestly renames the legacy
        # team-local relation. Its value and position in the legacy hash stay
        # unchanged, preserving all historical fallback event IDs.
        legacy_hash_fields = tuple(
            "related_event_id" if field == "related_team_event_id" else field
            for field in hash_fields
        )
        assert legacy_hash_fields == _MIGRATION_EVENT_NATURAL_KEY
        assert "_ingested_at" not in hash_fields

        contract_tree = ast.parse(
            (PROJECT_ROOT / "scripts" / "whoscored_v2_object_contract.py").read_text(
                encoding="utf-8"
            )
        )
        legacy_migration_keys = next(
            ast.literal_eval(node.value)
            for node in contract_tree.body
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "LEGACY_MIGRATION_KEYS"
        )
        assert (
            tuple(legacy_migration_keys["whoscored_events"])
            == _MIGRATION_EVENT_NATURAL_KEY
        )

    def test_output_columns_remain_frozen(self, duck_conn):
        row = _seed_and_run(duck_conn, [_row()])[0]
        assert list(row) == [
            "event_id",
            "match_id",
            "source_event_id_raw",
            "team_event_id_raw",
            "related_team_event_id_raw",
            "team_id_raw",
            "team_name_raw",
            "player_id_raw",
            "related_player_id_raw",
            "period",
            "expanded_minute",
            "minute",
            "second",
            "x",
            "y",
            "end_x",
            "end_y",
            "action_canonical",
            "action_source",
            "action_version",
            "_action_source_note",
            "_action_confidence",
            "outcome_success",
            "qualifiers_raw",
            "_bronze_ingested_at",
            "league",
            "season",
        ]

    def test_legacy_ids_do_not_depend_on_input_order(self, duck_conn):
        rows = [
            _row(
                game_id=700,
                period="PenaltyShootout",
                minute=120,
                second=0,
                type_="Pass",
            ),
            _row(game_id=700, period="FirstHalf", minute=10, second=0, type_="Pass"),
            _row(
                game_id=700,
                period="SecondPeriodOfExtraTime",
                minute=115,
                second=0,
                type_="Pass",
            ),
            _row(game_id=700, period="SecondHalf", minute=80, second=0, type_="Pass"),
            _row(
                game_id=700,
                period="FirstPeriodOfExtraTime",
                minute=100,
                second=0,
                type_="Pass",
            ),
        ]
        forward = {
            (r["period"], r["minute"]): r["event_id"]
            for r in _seed_and_run(duck_conn, rows)
        }
        reversed_input = {
            (r["period"], r["minute"]): r["event_id"]
            for r in _seed_and_run(duck_conn, list(reversed(rows)))
        }
        assert forward == reversed_input
        chronological = [
            "FirstHalf",
            "SecondHalf",
            "FirstPeriodOfExtraTime",
            "SecondPeriodOfExtraTime",
            "PenaltyShootout",
        ]
        sequence = [
            int(
                forward[
                    (
                        period,
                        next(row["minute"] for row in rows if row["period"] == period),
                    )
                ].split("_")[1]
            )
            for period in chronological
        ]
        assert sequence == sorted(sequence)

    def test_legacy_ties_do_not_depend_on_input_order(self, duck_conn):
        # Every pre-existing chronological sort field is identical. Only
        # migration-natural-key fields differ, so this specifically proves the
        # final SHA-256 tie-breaker rather than the ordinary clock ordering.
        rows = [
            _row(
                game_id=702,
                period="FirstHalf",
                minute=10,
                second=7,
                expanded_minute=10,
                type_="Pass",
                x=42.0,
                y=31.0,
                outcome_type="Successful",
                team_id=13,
                player_id=555,
                end_x=61.0,
                end_y=32.0,
                qualifiers=_qualifiers_json(["Cross"]),
                related_team_event_id=80,
                related_player_id=556,
                team="Arsenal",
                ingested_at="2026-05-01 10:00:00",
            ),
            _row(
                game_id=702,
                period="FirstHalf",
                minute=10,
                second=7,
                expanded_minute=10,
                type_="Pass",
                x=42.0,
                y=31.0,
                outcome_type="Unsuccessful",
                team_id=14,
                player_id=777,
                end_x=25.0,
                end_y=12.0,
                qualifiers=_qualifiers_json(["ThrowIn"]),
                related_team_event_id=81,
                related_player_id=778,
                team="Chelsea",
                ingested_at="2026-07-11 10:00:00",
            ),
        ]

        def mapping(fixture_rows):
            return {
                row["player_id_raw"]: row["event_id"]
                for row in _seed_and_run(duck_conn, fixture_rows)
            }

        forward = mapping(rows)
        reversed_input = mapping(list(reversed(rows)))
        assert forward == reversed_input
        assert set(forward.values()) == {"702_00001", "702_00002"}

    def test_legacy_sequence_orders_minutes_within_period(self, duck_conn):
        rows = [
            _row(
                game_id=701,
                period="FirstHalf",
                minute=30,
                second=0,
                expanded_minute=30,
                type_="Pass",
            ),
            _row(
                game_id=701,
                period="FirstHalf",
                minute=5,
                second=0,
                expanded_minute=5,
                type_="Pass",
            ),
            _row(
                game_id=701,
                period="FirstHalf",
                minute=15,
                second=0,
                expanded_minute=15,
                type_="Pass",
            ),
        ]
        out = _seed_and_run(duck_conn, rows)
        ordered = sorted(out, key=lambda row: int(row["event_id"].split("_")[1]))
        assert [row["expanded_minute"] for row in ordered] == [5, 15, 30]
