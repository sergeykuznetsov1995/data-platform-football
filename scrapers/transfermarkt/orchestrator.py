"""Run core and section targets from a scope plan through verified receipts.

No BaseScraper, no legacy materialisers. One scope unit in, one report with
outcomes, DQ and (when a writer is configured) a physically verified
mono-native receipt out. The fetch boundary is a callable with the client's
signature, so live transport and offline capture replay run the same code.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
from typing import Any
from urllib.parse import urlsplit

from . import attendance, core, native_writer, parsing, sections
from .models import FetchOutcome, FetchStatus


class CoreRunError(RuntimeError):
    """The requested scope or its capture evidence cannot support a run."""


Fetch = Callable[..., FetchOutcome]
TM_BASE = "https://www.transfermarkt.com"
TM_API_BASE = "https://tmapi.transfermarkt.technology"


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


def attendance_url(slug: str, competition_id: str, saison_year: int) -> str:
    return f"{TM_BASE}/{slug}/besucherzahlen/wettbewerb/{competition_id}/saison_id/{saison_year}"


def club_values_url(slug: str, competition_id: str, stichtag: str = '') -> str:
    url = f"{TM_BASE}/{slug}/marktwerteverein/wettbewerb/{competition_id}"
    return f"{url}/stichtag/{stichtag}" if stichtag else url


def achievements_url(recipient_type: str, recipient_id: str, slug: str = '') -> str:
    if recipient_type == 'coach':
        return f"{TM_BASE}/{slug}/erfolge/trainer/{recipient_id}"
    return f"{TM_API_BASE}/{recipient_type}/{recipient_id}/achievement"


@dataclass(frozen=True)
class ClubTarget:
    club_id: str
    club_slug: str
    club_name: str = ""
    squad_url: str = ""
    coach_history_url: str = ""


@dataclass(frozen=True)
class AttendanceEditionTarget:
    competition_id: str
    saison_year: int
    slug: str = ''


@dataclass(frozen=True)
class ClubValueTarget:
    competition_id: str
    slug: str = ''
    stichtag: str = ''


@dataclass(frozen=True)
class AchievementHolderTarget:
    recipient_type: str
    recipient_id: str
    slug: str = ''


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
    attendance_editions: tuple[AttendanceEditionTarget, ...] = ()
    club_value_targets: tuple[ClubValueTarget, ...] = ()
    achievement_holders: tuple[AchievementHolderTarget, ...] = ()


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
    competition_id: str = ""
    edition_id: str = ""
    recipient_id: str = ""
    recipient_type: str = ""

    @property
    def as_json(self) -> bool:
        return bool(self.player_id) or self.label in ("player_achievement", "club_achievement")

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
        if self.label == "attendance":
            return {"competition_id": self.competition_id, "edition_id": self.edition_id}
        if self.label == "club_values":
            return {"competition_id": self.competition_id}
        if self.label.endswith("_achievement"):
            return {"recipient_type": self.recipient_type, "recipient_id": self.recipient_id}
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
    for target in plan.attendance_editions:
        url = attendance_url(target.slug or target.competition_id, target.competition_id, target.saison_year)
        yield _Request("attendance", url, competition_id=target.competition_id, edition_id=str(target.saison_year))
    for target in plan.club_value_targets:
        url = club_values_url(target.slug or target.competition_id, target.competition_id, target.stichtag)
        yield _Request("club_values", url, competition_id=target.competition_id)
    for holder in plan.achievement_holders:
        if holder.recipient_type not in ("player", "club", "coach"):
            raise CoreRunError("achievements:invalid_recipient_type")
        if holder.recipient_type == "coach" and not holder.slug:
            raise CoreRunError("achievements:coach_slug_missing")
        url = achievements_url(holder.recipient_type, holder.recipient_id, holder.slug)
        yield _Request(f"{holder.recipient_type}_achievement", url,
                       recipient_type=holder.recipient_type, recipient_id=holder.recipient_id)


def planned_urls(plan: CoreScopePlan) -> tuple[str, ...]:
    return tuple(request.url for request in _requests(plan))


def _capture(request, result, raw_loader):
    if raw_loader is not None:
        body, record = raw_loader(result.raw_capture_id)
        if record.content_hash != result.raw_body_hash or record.url != request.url:
            raise CoreRunError(f"{request.label} raw capture does not match its outcome")
        captured_at = _normalise_ts(record.fetched_at)
    elif request.as_json:
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
    "attendance": ("attendance_observations",),
    "club_values": ("club_value_observations",),
    "player_achievement": ("achievement_observations",),
    "club_achievement": ("achievement_observations",),
    "coach_achievement": ("achievement_observations",),
}
_SUCCESS = {FetchStatus.OK, FetchStatus.AUTHORITATIVE_EMPTY, FetchStatus.NOT_APPLICABLE}


def _observations(request, result, plan, extracted_at, universe_version, raw_loader, captures=()):
    scope = core.ScopeContext(
        league=plan.league, season=plan.season, scope_id=plan.scope_id, cycle_id=plan.cycle_id,
        edition_id=request.edition_id or plan.edition_id or None, player_id=request.player_id or None,
        club_id=request.club.club_id if request.club else None,
        club_name=request.club.club_name if request.club else None,
    )
    metadata = {"extracted_at": extracted_at, "universe_version": universe_version}
    run_id = core._run_id(scope, extracted_at)
    if result.status is FetchStatus.NOT_APPLICABLE and request.label != "attendance":
        return tuple(
            core.captureless_outcome(
                entity, scope, status="not_applicable",
                reason=f"{request.label}:transport_not_applicable",
                proof=f"transport:not_applicable:{request.label}",
                run_id=run_id, **metadata)
            for entity in _ENTITIES[request.label]
        ), None
    if (result.status not in _SUCCESS or not result.raw_capture_id
            or (request.label == "attendance" and result.status is not FetchStatus.OK)):
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

    if request.label == "attendance":
        outcome = sections.extract_attendance_observations(
            captures, scope, start_url=request.url, competition_id=request.competition_id,
            edition_id=request.edition_id, **metadata,
        )
        return (outcome,), max(page.captured_at for page in captures)

    capture = _capture(request, result, raw_loader)
    if request.label == "squad":
        unit = core.extract_squad_observations(capture, scope, **metadata)
    elif request.label == "coach_history":
        unit = core.extract_coach_stints(capture, scope, **metadata)
    elif request.player_id:
        unit = core.extract_career_observations(capture, scope, **metadata)
    elif request.label == "club_values":
        unit = sections.extract_club_value_observations(capture, scope, **metadata)
    elif request.label.endswith("_achievement"):
        unit = sections.extract_achievement_observations(
            capture, scope, recipient_type=request.recipient_type,
            recipient_id=request.recipient_id, **metadata,
        )
    else:
        unit = core.extract_coach_profile(capture, scope, **metadata)
    return (unit if isinstance(unit, (tuple, list)) else (unit,)), capture.captured_at


def _outcome_scope_values(outcome, request, plan):
    if not outcome.rows:
        return request.scope_values(plan)
    columns = core.CORE_SCOPE_COLUMNS[outcome.entity]
    values = {column: outcome.rows[0][column] for column in columns}
    if any(row[column] != values[column] for row in outcome.rows for column in columns):
        raise CoreRunError(f"{outcome.entity} rows span multiple scopes")
    return values


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
            as_json = request.as_json
            page = request
            captures = []
            while True:
                result = self._fetch(
                    page.url, label=request.label, as_json=as_json, context=context,
                    validator=parsing.endpoint_validator(request.label, as_json=as_json),
                )
                fetched += 1
                cache_hits += int(result.cache_hit)
                if (request.label != "attendance" or result.status is not FetchStatus.OK
                        or not result.raw_capture_id):
                    break
                capture = _capture(page, result, self._raw_loader)
                captures.append(capture)
                try:
                    next_url = attendance.next_attendance_url(capture.body, page.url)
                    if next_url is None:
                        break
                    start = urlsplit(request.url)
                    next_url = attendance._validate_attendance_url(
                        next_url, host=start.netloc, path=start.path,
                        competition=request.competition_id, edition=request.edition_id,
                    )
                except ValueError:
                    # The donor parser reports the precise error from these same pages.
                    break
                if any(previous.url == next_url for previous in captures):
                    break
                if len(captures) >= 100:
                    raise CoreRunError('attendance:pagination_limit')
                page = replace(request, url=next_url)
            unit, captured_at = _observations(
                request, result, plan, extracted_at, self._universe_version, self._raw_loader, captures)
            outcomes.extend(unit)
            if captured_at is not None and writer is not None and connection_factory is not None:
                entries = [
                    (item, captured_at, _outcome_scope_values(item, request, plan))
                    for item in unit if item.status in core.CONCLUSIVE_CORE_STATUSES
                ]
                if entries:
                    captured_units.append((unit[0].run_id, entries))

        bundles = []
        for run_id, entries in captured_units:
            plans = [core.plan_for_outcome(item, captured_at=captured_at, scope_values=values)
                     for item, captured_at, values in entries]
            bundles.append((run_id, plans))
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
