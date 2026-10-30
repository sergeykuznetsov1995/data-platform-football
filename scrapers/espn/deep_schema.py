"""Plays table; Summary publications never replace these rows."""
import pyarrow as pa
from .bronze_schema import _schema

PLAYS_TABLE = "espn_plays"
PLAYS_KEY = ("event_id", "play_id")
PLAYS_SCHEMA = _schema(
    pa.field("play_id", pa.string()), pa.field("type_id", pa.string()),
    pa.field("type_text", pa.string()), pa.field("text", pa.string()),
    pa.field("period", pa.int32()), pa.field("clock_value", pa.float64()),
    pa.field("clock_display", pa.string()), pa.field("team_id", pa.int64()),
    pa.field("participants_json", pa.string()),
    *(pa.field(name, pa.float64()) for name in ("x", "y", "x2", "y2", "goal_y", "goal_z", "xg", "xgot")),
    pa.field("flags_json", pa.string()),
)
