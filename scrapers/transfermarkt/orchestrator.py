"""Thin stage-B orchestration: plan -> fetch -> parse -> observations -> receipt.

No BaseScraper, no legacy materialisers. One scope unit in, one report with
outcomes, DQ and (when a writer is configured) a physically verified
mono-native receipt out. The fetch boundary is a callable with the client's
signature, so live transport and offline capture replay run the same code.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Any

from . import core, native_writer, parsing
from .models import FetchOutcome, FetchStatus


class CoreRunError(RuntimeError):
    """The requested scope or its capture evidence cannot support a run."""


Fetch = Callable[..., FetchOutcome]
TM_BASE = "https://www.transfermarkt.com"


def squad_url(club_slug: str, club_id: str, saison_year: int) -> str:
    return f"{TM_BASE}/{club_slug}/kader/verein/{club_id}/saison_id/{saison_year}/plus/1"


def coach_history_url(club_slug: str, club_id: str) -> str:
    return f"{TM_BASE}/{club_slug}/mitarbeiterhistorie/verein/{club_id}/plus/1"


def coach_profile_url(coach_slug: str, coach_id: str) -> str:
    return f"{TM_BASE}/{coach_slug}/profil/trainer/{coach_id}"


def mv_history_url(player_id: str) -> str:
    return f"{TM_BASE}/ceapi/marketValueDevelopment/graph/{player_id}"


def transfers_url(player_id: str) -> str:
    return f"{TM_BASE}/ceapi/transferHistory/list/{player_id}"


@dataclass(frozen=True)
class ClubTarget:
    club_id: str
    club_slug: str
    club_name: str = ""
    squad_url: str = ""
    coach_history_url: str = ""


@dataclass(frozen=True)
class CoreScopePlan:
    league: str
    season: str
    scope_id: str
    cycle_id: str
    edition_id: str = ""
    clubs: tuple[ClubTarget, ...] = ()
    market_value_player_ids: tuple[str, ...] = ()
    transfer_player_ids: tuple[str, ...] = ()
    coach_profile_ids: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class CoreRunReport:
    run_id: str
    scope_id: str
    cycle_id: str
    outcomes: tuple[dict, ...]
    dq: dict
    receipt: dict | None
    fetched: int
    cache_hits: int
    receipts: tuple[dict, ...] = ()


def _normalise_ts(value: str) -> str:
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class _Request:
    label: str
    url: str
    club: ClubTarget | None = None
    player_id: str = ""
    coach_id: str = ""

    def scope_values(self, plan):
        # The replacement scope follows the entity's own scope columns:
        # squad lanes replace (league, season, club_id), stints replace the
        # club's whole history, career and coach profiles the single id.
        if self.label == "squad":
            return {"league": plan.league, "season": plan.season, "club_id": self.club.club_id}
        if self.label == "coach_history":
            return {"club_id": self.club.club_id}
        if self.player_id:
            return {"player_id": self.player_id}
        if self.coach_id:
            return {"coach_id": self.coach_id}
        raise CoreRunError(f"{self.label} request has no replacement scope")


def _requests(plan: CoreScopePlan):
    for club in plan.clubs:
        if club.squad_url:
            yield _Request("squad", club.squad_url, club=club)
        if club.coach_history_url:
            yield _Request("coach_history", club.coach_history_url, club=club)
    for player_id in plan.market_value_player_ids:
        yield _Request("market_value_points", mv_history_url(player_id), player_id=player_id)
    for player_id in plan.transfer_player_ids:
        yield _Request("transfer_events", transfers_url(player_id), player_id=player_id)
    for coach_id, slug in plan.coach_profile_ids:
        yield _Request("coach_profile", coach_profile_url(slug, coach_id), coach_id=coach_id)


def planned_urls(plan: CoreScopePlan) -> tuple[str, ...]:
    return tuple(request.url for request in _requests(plan))


def _capture(request, result, raw_loader):
    if raw_loader is not None:
        body, record = raw_loader(result.raw_capture_id)
        if record.content_hash != result.raw_body_hash or record.url != request.url:
            raise CoreRunError(f"{request.label} raw capture does not match its outcome")
        captured_at = _normalise_ts(record.fetched_at)
    elif request.player_id:
        # A JSON payload cannot be re-serialised to its exact source bytes.
        raise CoreRunError(f"{request.label} capture requires its raw body loader")
    else:
        body = result.value.encode("utf-8")
        if hashlib.sha256(body).hexdigest() != result.raw_body_hash:
            raise CoreRunError(f"{request.label} body does not match its hash")
        captured_at = _normalise_ts(result.raw_fetched_at)
    return core.SourceCapture(
        url=request.url, endpoint=request.label, body=body,
        capture_id=result.raw_capture_id, body_hash=result.raw_body_hash,
        captured_at=captured_at, status_code=result.status_code or 200,
    )


def _summary(outcome):
    return {
        "entity": outcome.entity,
        "status": outcome.status,
        "status_reason": outcome.status_reason,
        "rows": len(outcome.rows),
        "scope_id": outcome.scope.scope_id,
        "cycle_id": outcome.scope.cycle_id,
        "source_capture_id": outcome.capture.capture_id if outcome.capture else None,
        "observation_status": dict(Counter(
            row["observation_status"] for row in outcome.rows if row.get("observation_status")
        )),
        "run_id": outcome.run_id,
        "extracted_at": outcome.extracted_at,
    }



_ENTITIES = {
    "squad": ("squad_memberships", "player_attribute_observations", "player_contract_observations"),
    "coach_history": ("coach_stints",),
    "market_value_points": ("market_value_points",),
    "transfer_events": ("transfer_events",),
    "coach_profile": ("coach_profiles",),
}
_SUCCESS = {FetchStatus.OK, FetchStatus.AUTHORITATIVE_EMPTY, FetchStatus.NOT_APPLICABLE}


def _observations(request, result, plan, extracted_at, universe_version, raw_loader):
    scope = core.ScopeContext(
        league=plan.league, season=plan.season, scope_id=plan.scope_id, cycle_id=plan.cycle_id,
        edition_id=plan.edition_id or None, player_id=request.player_id or None,
        club_id=request.club.club_id if request.club else None,
        club_name=request.club.club_name if request.club else None,
    )
    metadata = {"extracted_at": extracted_at, "universe_version": universe_version}
    run_id = core._run_id(scope, extracted_at)
    if result.status is FetchStatus.NOT_APPLICABLE:
        return tuple(
            core.captureless_outcome(
                entity, scope, status="not_applicable",
                reason=f"{request.label}:transport_not_applicable",
                proof=f"transport:not_applicable:{request.label}",
                run_id=run_id, **metadata)
            for entity in _ENTITIES[request.label]
        ), None
    if result.status not in _SUCCESS or not result.raw_capture_id:
        status = "unavailable"
        reason = f"{request.label}:{result.status.value}"
        if request.player_id:
            reason = f"career:{reason}"
            if result.status in _SUCCESS:
                status = "error"
                reason = f"career:{request.label}:no_capture"
        return tuple(
            core.captureless_outcome(entity, scope, status=status, reason=reason,
                                     run_id=run_id, **metadata)
            for entity in _ENTITIES[request.label]
        ), None

    capture = _capture(request, result, raw_loader)
    if request.label == "squad":
        unit = core.extract_squad_observations(capture, scope, **metadata)
    elif request.label == "coach_history":
        unit = core.extract_coach_stints(capture, scope, **metadata)
    elif request.player_id:
        unit = core.extract_career_observations(capture, scope, **metadata)
    else:
        unit = core.extract_coach_profile(capture, scope, **metadata)
    return (unit if isinstance(unit, (tuple, list)) else (unit,)), capture.captured_at


class CoreRun:
    def __init__(
        self,
        fetch: Fetch,
        *,
        raw_loader: Callable[[str], tuple[bytes, Any]] | None = None,
        universe_version: str = "core-universe-v1",
        extracted_at: str | None = None,
    ):
        self._fetch = fetch
        self._raw_loader = raw_loader
        self._universe_version = universe_version
        self._extracted_at = extracted_at

    def run(self, plan: CoreScopePlan, *, writer=None, connection_factory=None) -> CoreRunReport:
        extracted_at = (
            datetime.now(timezone.utc).isoformat(timespec="microseconds")
            if self._extracted_at is None else _normalise_ts(self._extracted_at)
        )
        context = {"scope_id": plan.scope_id, "cycle_id": plan.cycle_id}
        outcomes = []
        captured_units = []
        fetched = cache_hits = 0
        for request in _requests(plan):
            as_json = bool(request.player_id)
            result = self._fetch(
                request.url, label=request.label, as_json=as_json, context=context,
                validator=parsing.endpoint_validator(request.label, as_json=as_json),
            )
            fetched += 1
            cache_hits += int(result.cache_hit)
            unit, captured_at = _observations(
                request, result, plan, extracted_at, self._universe_version, self._raw_loader)
            outcomes.extend(unit)
            if captured_at is not None and writer is not None and connection_factory is not None:
                captured_units.append((unit, captured_at, request.scope_values(plan)))

        bundles = []
        for unit, captured_at, values in captured_units:
            conclusive = [item for item in unit if item.status in core.CONCLUSIVE_CORE_STATUSES]
            if conclusive:
                plans = [
                    core.plan_for_outcome(item, captured_at=captured_at, scope_values=values)
                    for item in conclusive
                ]
                bundles.append((conclusive[0].run_id, plans))
        receipts = []
        for run_id, plans in bundles:
            receipt = native_writer.write_core_bundle(
                connection_factory, writer, plans, run_id=run_id,
                universe_version=self._universe_version, committed_at=extracted_at,
            )
            if not isinstance(receipt, dict):
                raise CoreRunError(f"Missing receipt for capture run {run_id}")
            receipts.append(receipt)
        verified = None
        if receipts:
            verified = {
                "verified": all(item.get("verified") is True for item in receipts),
                "receipt_readback": all(item.get("receipt_readback") is True for item in receipts),
                "plan_sha256": "|".join(item.get("plan_sha256", "") for item in receipts),
                "committed_batch": receipts[0].get("committed_batch", ""),
            }
        dq = core.build_core_dq(outcomes, verified_receipt=verified)
        return CoreRunReport(
            run_id=outcomes[0].run_id if outcomes else "",
            scope_id=plan.scope_id,
            cycle_id=plan.cycle_id,
            outcomes=tuple(_summary(item) for item in outcomes),
            dq=dq,
            receipt=receipts[0] if receipts else None,
            receipts=tuple(receipts),
            fetched=fetched,
            cache_hits=cache_hits,
        )
