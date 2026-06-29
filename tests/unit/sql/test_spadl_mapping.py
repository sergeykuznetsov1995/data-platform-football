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
* Synthetic ``event_id`` uniqueness within a game.
* Enum completeness vs ``utils.e3_dq.SPADL_ACTION_ENUM``.
"""

from __future__ import annotations

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
      * ``iceberg.bronze.whoscored_events`` → ``bronze_whoscored_events``
        (single-namespace table seeded by the fixture).
      * ``regexp_like(<col>, <pat>)`` → ``regexp_matches(<col>, <pat>)`` —
        DuckDB's idiomatic spelling. Both treat the second argument as a
        POSIX-extended regex, so ``\\s*`` etc. work identically.
      * ``LPAD`` / ``TRY_CAST`` / ``ROW_NUMBER OVER`` / ``COALESCE`` —
        already DuckDB-compatible, no rewrite needed.
    """
    # 1. Source table reference.
    sql = sql.replace(
        "iceberg.bronze.whoscored_events",
        "bronze_whoscored_events",
    )

    # 2. regexp_like → regexp_matches (both functions take same args).
    sql = re.sub(
        r"\bregexp_like\s*\(",
        "regexp_matches(",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


# Bronze schema mirrors the columns the Silver SELECT consumes from
# ``iceberg.bronze.whoscored_events`` (per file header L189-L216).
_BRONZE_COLUMNS: List[str] = [
    "game_id", "period", "minute", "second", "expanded_minute",
    "type", "outcome_type", "team_id", "player_id",
    "x", "y", "end_x", "end_y",
    "qualifiers", "related_event_id", "related_player_id", "team",
    "league", "season", "_ingested_at",
]


def _row(
    *,
    game_id: int = 100,
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
    related_event_id: Optional[int] = None,
    related_player_id: Optional[int] = None,
    team: str = "Arsenal",
    league: str = "ENG-Premier League",
    season: str = "2526",
    ingested_at: str = "2026-05-08 12:00:00",
) -> Dict[str, Any]:
    """Build a single bronze fixture row with sensible defaults."""
    return {
        "game_id": game_id,
        "period": period,
        "minute": minute,
        "second": second,
        "expanded_minute": expanded_minute,
        "type": type_,
        "outcome_type": outcome_type,
        "team_id": team_id,
        "player_id": player_id,
        "x": x, "y": y, "end_x": end_x, "end_y": end_y,
        "qualifiers": qualifiers,
        "related_event_id": related_event_id,
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
            related_event_id  BIGINT,
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
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["ThrowIn"])),
        ])
        assert out[0]["action_canonical"] == "throw_in"
        assert out[0]["_action_confidence"] == "medium"

    def test_goalkick(self, duck_conn):
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["GoalKick"])),
        ])
        assert out[0]["action_canonical"] == "goalkick"

    def test_corner_crossed(self, duck_conn):
        """CornerTaken + Cross → corner_crossed."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["CornerTaken", "Cross"])),
        ])
        assert out[0]["action_canonical"] == "corner_crossed"

    def test_corner_short(self, duck_conn):
        """CornerTaken without Cross → corner_short."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["CornerTaken"])),
        ])
        assert out[0]["action_canonical"] == "corner_short"

    def test_freekick_crossed(self, duck_conn):
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["FreekickTaken", "Cross"])),
        ])
        assert out[0]["action_canonical"] == "freekick_crossed"

    def test_freekick_short(self, duck_conn):
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["FreekickTaken"])),
        ])
        assert out[0]["action_canonical"] == "freekick_short"

    def test_open_cross(self, duck_conn):
        """Cross qualifier (no other) → cross."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=_qualifiers_json(["Cross"])),
        ])
        assert out[0]["action_canonical"] == "cross"

    def test_default_no_qualifiers(self, duck_conn):
        """Pass with NULL qualifiers → 'pass' fallback (medium confidence)."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=None),
        ])
        assert out[0]["action_canonical"] == "pass"
        assert out[0]["_action_confidence"] == "medium"

    def test_empty_qualifiers_string(self, duck_conn):
        """Pass with empty-string qualifiers → 'pass' fallback."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers=""),
        ])
        assert out[0]["action_canonical"] == "pass"

    def test_empty_array_qualifiers(self, duck_conn):
        """Pass with '[]' literal → 'pass' fallback (per CASE branch)."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Pass", qualifiers="[]"),
        ])
        assert out[0]["action_canonical"] == "pass"

    def test_offside_pass(self, duck_conn):
        """OffsidePass type → 'pass' (still pass, low confidence)."""
        out = _seed_and_run(duck_conn, [
            _row(type_="OffsidePass", qualifiers=None),
        ])
        assert out[0]["action_canonical"] == "pass"
        assert out[0]["_action_confidence"] == "low"


# ---------------------------------------------------------------------------
# Direct SPADL matches
# ---------------------------------------------------------------------------


class TestDirectMatches:
    """type → action_canonical direct CASE branches (high confidence)."""

    @pytest.mark.parametrize("ws_type, expected", [
        ("Foul", "foul"),
        ("TakeOn", "take_on"),
        ("Tackle", "tackle"),
        ("Interception", "interception"),
        ("BlockedPass", "interception"),
        ("Challenge", "tackle"),
        ("Clearance", "clearance"),
    ])
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

    @pytest.mark.parametrize("ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"])
    def test_shot_default(self, duck_conn, ws_type):
        out = _seed_and_run(duck_conn, [_row(type_=ws_type, qualifiers=None)])
        assert out[0]["action_canonical"] == "shot"

    @pytest.mark.parametrize("ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"])
    def test_shot_penalty(self, duck_conn, ws_type):
        out = _seed_and_run(duck_conn, [
            _row(type_=ws_type, qualifiers=_qualifiers_json(["Penalty"])),
        ])
        assert out[0]["action_canonical"] == "shot_penalty"

    @pytest.mark.parametrize("ws_type", ["SavedShot", "MissedShots", "ShotOnPost", "Goal"])
    def test_shot_freekick(self, duck_conn, ws_type):
        """DirectFreekick qualifier on shot → shot_freekick.

        Free-kick *shots* carry `DirectFreekick`; `FreekickTaken` only tags the
        pass set-piece and never appears on a shot event.
        """
        out = _seed_and_run(duck_conn, [
            _row(type_=ws_type, qualifiers=_qualifiers_json(["DirectFreekick"])),
        ])
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
        out = _seed_and_run(duck_conn, [
            _row(type_="Goal", qualifiers=_qualifiers_json(["OwnGoal"])),
        ])
        assert out[0]["action_canonical"] == "own_goal"

    def test_plain_goal_still_routes_to_shot(self, duck_conn):
        """Guard against over-matching: a Goal without the OwnGoal qualifier
        must still land in the shot family (#462 behaviour preserved)."""
        out = _seed_and_run(duck_conn, [
            _row(type_="Goal", qualifiers=None),
        ])
        assert out[0]["action_canonical"] == "shot"

    def test_own_goal_in_enum(self, duck_conn):
        """'own_goal' must be a registered SPADL_ACTION_ENUM value or the
        DQ enum-violation guard would hard-fail on it."""
        from utils.e3_dq import SPADL_ACTION_ENUM
        assert "own_goal" in SPADL_ACTION_ENUM
        out = _seed_and_run(duck_conn, [
            _row(type_="Goal", qualifiers=_qualifiers_json(["OwnGoal"])),
        ])
        assert out[0]["action_canonical"] in set(SPADL_ACTION_ENUM)


# ---------------------------------------------------------------------------
# Goalkeeper actions
# ---------------------------------------------------------------------------


class TestGoalkeeperActions:
    @pytest.mark.parametrize("ws_type, expected", [
        ("Save", "keeper_save"),
        ("KeeperSweeper", "keeper_save"),
        ("Smother", "keeper_save"),
        ("PenaltyFaced", "keeper_save"),
        ("KeeperPickup", "keeper_pick_up"),
        ("Punch", "keeper_punch"),
        ("Claim", "keeper_claim"),
        ("CrossNotClaimed", "keeper_claim"),
    ])
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
        out = _seed_and_run(duck_conn, [
            _row(type_="BallTouch", outcome_type="Unsuccessful"),
        ])
        assert out[0]["action_canonical"] == "bad_touch"
        assert out[0]["outcome_success"] is False

    def test_ball_touch_successful(self, duck_conn):
        """BallTouch with outcome_type='Successful' → bad_touch (parity-preserving;
        R3.D2 originally proposed dropping but kept for row-count parity).
        """
        out = _seed_and_run(duck_conn, [
            _row(type_="BallTouch", outcome_type="Successful"),
        ])
        assert out[0]["action_canonical"] == "bad_touch"
        assert out[0]["outcome_success"] is True

    @pytest.mark.parametrize("ws_type, expected", [
        ("Dispossessed", "bad_touch"),
        ("Error", "bad_touch"),
        ("ShieldBallOpp", "dribble"),
        ("GoodSkill", "dribble"),
    ])
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


class TestSyntheticEventId:
    """event_id = game_id || '_' || LPAD(event_seq, 5, '0') is unique per game."""

    def test_event_id_format(self, duck_conn):
        out = _seed_and_run(duck_conn, [
            _row(game_id=200, minute=1, second=0, type_="Pass"),
        ])
        # LPAD(1, 5, '0') = '00001' with the dedup ROW_NUMBER assigning event_seq=1
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
        # Same minute/second but different games → different event_id prefixes.
        prefixes = {r["event_id"].split("_")[0] for r in out}
        assert prefixes == {"301", "302"}

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
        out = _seed_and_run(duck_conn, [
            _row(game_id=500, minute=47, second=12, type_="Pass"),
        ])
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
# Deduplication of bronze re-scrapes (ROW_NUMBER on natural key)
# ---------------------------------------------------------------------------


class TestBronzeDedup:
    """When two bronze rows share the full natural key, only the freshest survives."""

    def test_keeps_latest_ingested(self, duck_conn):
        # Same natural key (15-col tuple) but different _ingested_at timestamps.
        rows = [
            _row(
                game_id=500, minute=1, second=0,
                type_="Pass", qualifiers=_qualifiers_json(["Cross"]),
                ingested_at="2026-05-01 10:00:00",
            ),
            _row(
                game_id=500, minute=1, second=0,
                type_="Pass", qualifiers=_qualifiers_json(["Cross"]),
                ingested_at="2026-05-08 10:00:00",  # newer
            ),
        ]
        out = _seed_and_run(duck_conn, rows)
        # Dedup collapses the duplicate.
        assert len(out) == 1
        assert out[0]["action_canonical"] == "cross"


# ---------------------------------------------------------------------------
# Confidence cascade
# ---------------------------------------------------------------------------


class TestConfidenceCascade:
    """The confidence label distribution per file header L342-364."""

    @pytest.mark.parametrize("ws_type, conf", [
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
    ])
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
            "Pass", "BallRecovery", "BallTouch", "Aerial", "Clearance",
            "Foul", "TakeOn", "Tackle", "CornerAwarded", "Dispossessed",
            "Interception", "BlockedPass", "Challenge", "SavedShot",
            "Save", "KeeperPickup", "MissedShots", "SubstitutionOff",
            "SubstitutionOn", "End", "Card", "Start", "OffsideProvoked",
            "OffsideGiven", "OffsidePass", "FormationChange", "Goal",
            "FormationSet", "Claim", "Error", "ShieldBallOpp",
            "KeeperSweeper", "Punch", "ShotOnPost", "Smother",
            "PenaltyFaced", "GoodSkill", "ChanceMissed", "CrossNotClaimed",
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
            _row(game_id=1000, minute=1, type_="Pass",
                 qualifiers=_qualifiers_json(["ThrowIn"])),
            _row(game_id=1000, minute=2, type_="Pass",
                 qualifiers=_qualifiers_json(["GoalKick"])),
            _row(game_id=1000, minute=3, type_="Pass",
                 qualifiers=_qualifiers_json(["CornerTaken", "Cross"])),
            _row(game_id=1000, minute=4, type_="Pass",
                 qualifiers=_qualifiers_json(["CornerTaken"])),
            _row(game_id=1000, minute=5, type_="Pass",
                 qualifiers=_qualifiers_json(["FreekickTaken", "Cross"])),
            _row(game_id=1000, minute=6, type_="Pass",
                 qualifiers=_qualifiers_json(["FreekickTaken"])),
            _row(game_id=1000, minute=7, type_="Pass",
                 qualifiers=_qualifiers_json(["Cross"])),
            _row(game_id=1000, minute=8, type_="SavedShot",
                 qualifiers=_qualifiers_json(["Penalty"])),
            _row(game_id=1000, minute=9, type_="SavedShot",
                 qualifiers=_qualifiers_json(["DirectFreekick"])),
        ]
        out = _seed_and_run(duck_conn, rows)
        observed = {r["action_canonical"] for r in out}
        leaked = observed - allowed
        assert not leaked, (
            f"qualifier branch leaked outside enum: {sorted(leaked)}"
        )


# ---------------------------------------------------------------------------
# event_seq chronology across periods (#477)
# ---------------------------------------------------------------------------


class TestEventSeqChronology:
    """event_seq must follow true match chronology, NOT lexical period order.

    Regression for #477: the seq CTE used ``ORDER BY period`` where period is a
    VARCHAR. Lexically 'FirstPeriodOfExtraTime' < 'PenaltyShootout' < 'SecondHalf'
    < 'SecondPeriodOfExtraTime', so cup matches with extra time / shootouts got a
    non-monotonic event_seq (extra-time/shootout events sorted BEFORE the second
    half). The fix replaces the raw period column with an explicit chronological
    CASE ordinal.
    """

    @staticmethod
    def _seq(event_id: str) -> int:
        return int(event_id.split("_")[1])

    def test_extra_time_and_shootout_ordered_after_second_half(self, duck_conn):
        # Seeded in deliberately NON-chronological insert order; the SQL must
        # still assign event_seq following real match time.
        rows = [
            _row(game_id=700, period="PenaltyShootout",         minute=120, second=0, type_="Pass"),
            _row(game_id=700, period="FirstHalf",               minute=10,  second=0, type_="Pass"),
            _row(game_id=700, period="SecondPeriodOfExtraTime", minute=115, second=0, type_="Pass"),
            _row(game_id=700, period="SecondHalf",              minute=80,  second=0, type_="Pass"),
            _row(game_id=700, period="FirstPeriodOfExtraTime",  minute=100, second=0, type_="Pass"),
        ]
        out = _seed_and_run(duck_conn, rows)
        seq = {r["period"]: self._seq(r["event_id"]) for r in out}
        chronological = [
            "FirstHalf", "SecondHalf", "FirstPeriodOfExtraTime",
            "SecondPeriodOfExtraTime", "PenaltyShootout",
        ]
        ordered = [seq[p] for p in chronological]
        assert ordered == sorted(ordered), (
            f"event_seq not monotonic with match chronology: {seq}"
        )

    def test_within_period_minute_order_preserved(self, duck_conn):
        """Within a single period, event_seq still follows ascending minute."""
        rows = [
            _row(game_id=701, period="FirstHalf", minute=30, second=0, expanded_minute=30, type_="Pass"),
            _row(game_id=701, period="FirstHalf", minute=5,  second=0, expanded_minute=5,  type_="Pass"),
            _row(game_id=701, period="FirstHalf", minute=15, second=0, expanded_minute=15, type_="Pass"),
        ]
        out = _seed_and_run(duck_conn, rows)
        ordered = sorted(out, key=lambda r: self._seq(r["event_id"]))
        minutes = [r["expanded_minute"] for r in ordered]
        assert minutes == [5, 15, 30], f"within-period order broken: {minutes}"
