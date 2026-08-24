"""Fail-closed exact scopes for the source-native SofaScore history campaign."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from scrapers.sofascore.catalog import SofaScoreCatalog


class CampaignScopeError(ValueError):
    """A campaign snapshot cannot authorize one exact paid capture scope."""


def campaign_policy_id(policy: Mapping[str, Any]) -> str:
    return _digest(policy)


def candidate_ids_digest(rows: object) -> str:
    if not isinstance(rows, list):
        raise CampaignScopeError("campaign snapshot tournaments must be a list")
    ids = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise CampaignScopeError("campaign tournament must be an object")
        ids.append(_positive_int(
            row.get("unique_tournament_id"), "unique_tournament_id"
        ))
    if len(ids) != len(set(ids)):
        raise CampaignScopeError("campaign tournament ids must be unique")
    return hashlib.sha256(json.dumps(
        sorted(ids), separators=(",", ":")
    ).encode()).hexdigest()


def validate_campaign_snapshot(
    document: Mapping[str, Any], policy: Mapping[str, Any]
) -> None:
    """Bind a mutable evidence revision to the committed campaign policy."""

    if str(document.get("snapshot_id") or "") != _snapshot_digest(document):
        raise CampaignScopeError("campaign snapshot digest mismatch")
    rows = document.get("tournaments")
    ids_digest = candidate_ids_digest(rows)
    expected_count = _positive_int(policy.get("candidate_count"), "candidate_count")
    if not isinstance(rows, list) or len(rows) != expected_count:
        raise CampaignScopeError("campaign candidate_count does not match policy")
    if document.get("candidate_count") != expected_count:
        raise CampaignScopeError("snapshot candidate_count does not match policy")
    expected_ids = str(policy.get("candidate_ids_sha256") or "")
    if not expected_ids or ids_digest != expected_ids:
        raise CampaignScopeError("campaign candidate id set does not match policy")
    expected_policy_id = campaign_policy_id(policy)
    if document.get("policy_id") != expected_policy_id:
        raise CampaignScopeError("campaign policy_id mismatch")
    expected_campaign_id = _digest({
        "policy_id": expected_policy_id,
        "candidate_ids_sha256": ids_digest,
    })
    if document.get("campaign_id") != expected_campaign_id:
        raise CampaignScopeError("campaign_id mismatch")


@dataclass(frozen=True)
class ScopeOverlayPaths:
    registry_path: Path
    competitions_path: Path


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise CampaignScopeError(f"{field} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise CampaignScopeError(f"{field} must be a positive integer") from exc
    if result <= 0:
        raise CampaignScopeError(f"{field} must be a positive integer")
    return result


def _snapshot_digest(document: Mapping[str, Any]) -> str:
    unsigned = dict(document)
    unsigned.pop("snapshot_id", None)
    return _digest(unsigned)


def _scope_digest(scope: Mapping[str, Any]) -> str:
    unsigned = dict(scope)
    unsigned.pop("scope_digest", None)
    return _digest(unsigned)


def load_exact_scope(
    snapshot_path: str | Path,
    *,
    tournament_id: int,
    source_season_id: int,
    expected_snapshot_id: str | None = None,
    expected_campaign_id: str | None = None,
    allow_pending_season: bool = False,
) -> dict[str, Any]:
    """Load one ready tournament-season and bind it to the snapshot digest.

    ``allow_pending_season`` admits a season whose metadata is still
    ``pending`` (no team pages, hence no ``team_count``/evidence): the refresh
    lane plans its matches from Bronze schedule evidence instead of season
    pages.  Tournament classification and eligibility checks stay as strict as
    for the history campaign; an ``excluded`` season is never admitted.
    """

    path = Path(snapshot_path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignScopeError(f"cannot read campaign snapshot {path}: {exc}") from exc
    if not isinstance(document, Mapping):
        raise CampaignScopeError("campaign snapshot root must be an object")
    snapshot_id = str(document.get("snapshot_id") or "")
    if snapshot_id != _snapshot_digest(document):
        raise CampaignScopeError("campaign snapshot digest mismatch")
    if expected_snapshot_id and snapshot_id != expected_snapshot_id:
        raise CampaignScopeError("campaign snapshot changed after planning")
    campaign_id = str(document.get("campaign_id") or "")
    if not campaign_id:
        raise CampaignScopeError("campaign_id is missing")
    if expected_campaign_id and campaign_id != expected_campaign_id:
        raise CampaignScopeError("campaign identity changed after planning")
    rows = document.get("tournaments")
    if not isinstance(rows, list):
        raise CampaignScopeError("campaign snapshot tournaments must be a list")
    wanted_tournament = _positive_int(tournament_id, "tournament_id")
    matches = [
        row for row in rows
        if isinstance(row, Mapping)
        and row.get("unique_tournament_id") == wanted_tournament
    ]
    if len(matches) != 1:
        raise CampaignScopeError(
            f"snapshot must contain exactly one tournament {wanted_tournament}"
        )
    tournament = matches[0]
    if tournament.get("metadata_status") != "ready":
        raise CampaignScopeError("tournament metadata_status must be ready")
    classification = tournament.get("classification")
    if not isinstance(classification, Mapping):
        raise CampaignScopeError("source classification is missing")
    if (
        classification.get("sport") != "football"
        or classification.get("gender") != "male"
        or classification.get("status") in {None, "unknown", "excluded"}
        or classification.get("exclusion_reasons")
    ):
        raise CampaignScopeError(
            "source classification must be confirmed male football without exclusions"
        )
    review = tournament.get("eligibility_review")
    confirmed = review.get("confirmed") if isinstance(review, Mapping) else None
    if (
        not isinstance(review, Mapping)
        or review.get("status") != "approved"
        or not isinstance(confirmed, Mapping)
        or confirmed.get("age_group") != "adult"
        or confirmed.get("team_level") != "first_team"
        or confirmed.get("professional") is not True
        or not str(review.get("reviewed_by") or "").strip()
    ):
        raise CampaignScopeError(
            "adult professional first-team eligibility review is missing"
        )
    seasons = tournament.get("seasons")
    wanted_season = _positive_int(source_season_id, "source_season_id")
    season_matches = [
        season for season in seasons or []
        if isinstance(season, Mapping)
        and season.get("source_season_id") == wanted_season
    ]
    if len(season_matches) != 1:
        raise CampaignScopeError(
            f"snapshot must contain exactly one source season {wanted_season}"
        )
    season = season_matches[0]
    pending = allow_pending_season and season.get("metadata_status") == "pending"
    if season.get("metadata_status") != "ready" and not pending:
        raise CampaignScopeError("season metadata_status must be ready")
    team_count = season.get("team_count")
    evidence = season.get("team_count_evidence")
    if pending and team_count is None:
        evidence = None
    else:
        team_count = _positive_int(team_count, "team_count")
        if (
            not isinstance(evidence, Mapping)
            or evidence.get("count") != team_count
            or not str(evidence.get("endpoint") or "").strip()
        ):
            raise CampaignScopeError(
                "team_count evidence is missing or inconsistent"
            )
        evidence = dict(evidence)
    canonical = str(season.get("canonical_season") or "").strip()
    if not canonical or not canonical.isdigit():
        raise CampaignScopeError("canonical_season must be numeric")
    season_format = str(season.get("season_format") or "").strip()
    if season_format not in {"split_year", "single_year"}:
        raise CampaignScopeError("season_format must be split_year or single_year")
    capture_key = str(tournament.get("capture_key") or "").strip()
    expected_key = f"SS-{wanted_tournament}"
    if capture_key != expected_key:
        raise CampaignScopeError(f"capture_key must equal {expected_key}")
    scope = {
        "snapshot_id": snapshot_id,
        "campaign_id": campaign_id,
        "policy_id": str(document.get("policy_id") or ""),
        "tournament_id": wanted_tournament,
        "source_season_id": wanted_season,
        "capture_key": capture_key,
        "name": str(tournament.get("name") or "").strip(),
        "slug": str(tournament.get("slug") or "").strip(),
        "page_path": str(tournament.get("page_path") or "").strip(),
        "category": dict(tournament.get("category") or {}),
        "kind": str(tournament.get("kind") or "").strip(),
        "classification": dict(classification),
        "eligibility_review": dict(review),
        "source_name": str(season.get("source_name") or canonical).strip(),
        "canonical_season": canonical,
        "season_format": season_format,
        "team_count": team_count,
        "team_count_evidence": evidence,
    }
    if not scope["name"] or not scope["slug"] or not scope["page_path"]:
        raise CampaignScopeError("tournament identity metadata is incomplete")
    if scope["kind"] not in {"league", "cup"}:
        raise CampaignScopeError("tournament kind must be league or cup")
    scope["scope_digest"] = _scope_digest(scope)
    return scope


PENDING_SEASON_TEAM_COUNT = 1


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _source_year(canonical: str, season_format: str) -> str:
    if season_format == "split_year" and len(canonical) == 4:
        return f"{canonical[:2]}/{canonical[2:]}"
    return canonical


def render_scope_overlays(
    scope: Mapping[str, Any], output_dir: str | Path
) -> ScopeOverlayPaths:
    """Render one-row registry/medallion overlays without global config growth."""

    if str(scope.get("scope_digest") or "") != _scope_digest(scope):
        raise CampaignScopeError("scope digest mismatch")
    destination = Path(output_dir)
    registry_path = destination / "tournaments.json"
    competitions_path = destination / "medallion" / "competitions.yaml"
    tournament_id = _positive_int(scope.get("tournament_id"), "tournament_id")
    source_season_id = _positive_int(
        scope.get("source_season_id"), "source_season_id"
    )
    team_count = scope.get("team_count")
    if team_count is not None:
        team_count = _positive_int(team_count, "team_count")
    canonical = str(scope.get("canonical_season") or "")
    season_format = str(scope.get("season_format") or "")
    source_name = str(scope.get("source_name") or canonical)
    category = dict(scope.get("category") or {})
    classification = dict(scope.get("classification") or {})
    source_year = _source_year(canonical, season_format)
    registry = {
        "schema_version": 2,
        "generated_at": "2026-08-21T00:00:00Z",
        "tournaments": [{
            "unique_tournament_id": tournament_id,
            "name": str(scope["name"]),
            "slug": str(scope["slug"]),
            "category": category,
            "sport_slug": "football",
            "page_path": str(scope["page_path"]),
            "canonical_id": str(scope["capture_key"]),
            "enabled": True,
            "classification": classification,
            "review": {
                **dict(scope["eligibility_review"]),
                "confirmed": {
                    "sport": "football",
                    "gender": "male",
                    "age_group": "adult",
                    "team_level": "first_team",
                },
                "evidence": [{
                    "type": "owner_scope_policy",
                    "reference": str(scope["snapshot_id"]),
                    "note": "adult men's professional leagues and cups",
                }],
                "notes": "source-native Bronze history campaign",
            },
            "seasons": [{
                "season_id": source_season_id,
                "name": source_name,
                "source_name": source_name,
                "year": source_year,
                "format": (
                    "split_year" if season_format == "split_year"
                    else "calendar_year"
                ),
                "season_format": season_format,
                "canonical_season": canonical,
                "start_date": None,
                "end_date": None,
                "aliases": list(dict.fromkeys((source_year, canonical, source_name))),
                "evidence": [{
                    "type": "campaign_snapshot",
                    "endpoint": f"/unique-tournament/{tournament_id}/seasons",
                    "value": str(scope["snapshot_id"]),
                }],
                **(
                    {
                        "team_count": team_count,
                        "team_count_evidence": dict(scope["team_count_evidence"]),
                    }
                    if team_count is not None
                    else {}
                ),
            }],
        }],
    }
    is_cup = scope.get("kind") == "cup"
    if team_count is None:
        # A pending season has no team pages yet, but competitions.yaml
        # demands a positive team_count (a DQ-floor invariant the campaign
        # overlay never consumes).  The placeholder sits below the measured
        # band grid, so page-evidence planning of such a season fails closed
        # in team_count_band() instead of authorizing an unmeasured class.
        team_count = PENDING_SEASON_TEAM_COUNT
    season_config: dict[str, Any] = {
        "id": int(canonical),
        "season_format": season_format,
        "format": "group_knockout" if is_cup else "league_round_robin",
        "team_count": team_count,
    }
    if is_cup:
        # A deliberately high provisional floor cannot silently pass campaign
        # DQ. Exact match totals are learned from the captured schedule.
        season_config["match_count"] = max(1, team_count * (team_count - 1))
    competitions = {
        "competitions": [{
            "id": str(scope["capture_key"]),
            "name": str(scope["name"]),
            "country": str(category.get("name") or "World"),
            "tier": 1,
            "competition_format": "group_knockout" if is_cup else "league",
            "seasons": [season_config],
            "sources": {"primary": ["sofascore"], "fallback": []},
            "in_scope": False,
            "notes": "source-native Bronze history campaign overlay",
        }]
    }
    _atomic_write(
        registry_path,
        (json.dumps(registry, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )
    _atomic_write(
        competitions_path,
        yaml.safe_dump(competitions, allow_unicode=True, sort_keys=False).encode(
            "utf-8"
        ),
    )
    SofaScoreCatalog.load(registry_path)
    return ScopeOverlayPaths(registry_path, competitions_path)


__all__ = [
    "CampaignScopeError",
    "ScopeOverlayPaths",
    "campaign_policy_id",
    "candidate_ids_digest",
    "load_exact_scope",
    "render_scope_overlays",
    "validate_campaign_snapshot",
]
