"""Prepare ESPN tournament-seasons independently and publish their ready Bronze rows."""

from __future__ import annotations

from concurrent.futures import CancelledError
from dataclasses import dataclass, replace
import logging
from typing import Iterable, Literal, Sequence

from scrapers.base.trino_manager import TrinoAmbiguousWriteError

from . import bronze_writer, ready_replace, recheck, wave
from .deep_store import client_state_dir
from .transport_contracts import AllOriginsBlocked, LaneClosed

logger = logging.getLogger(__name__)

EditionKey = tuple[str, int]
_INTERRUPTS = (TimeoutError, CancelledError)
MAX_GROUP_EVENTS = 40
MAX_GROUP_EDITIONS = 20


class GroupRunError(RuntimeError):
    def __init__(self, outcomes: list[wave.TournamentOutcome], *, no_retry: bool):
        self.outcomes = outcomes
        self.no_retry = no_retry
        first = next(item for item in outcomes if item.state == wave.RED)
        super().__init__(f"{first.slug}:{first.season_year}: {first.first_error}")


def _global_gate(exc: Exception) -> bool:
    return isinstance(exc, (AllOriginsBlocked, LaneClosed))


def _contains_error(exc: BaseException, types: tuple[type[BaseException], ...]) -> bool:
    pending = [exc]
    seen = set()
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        if isinstance(error, types):
            return True
        if isinstance(error, BaseExceptionGroup):
            pending.extend(error.exceptions)
        cause = error.__cause__ or error.__context__
        if cause is not None:
            pending.append(cause)
    return False


def ambiguous_write(exc: BaseException, *, journal_write: bool = False) -> bool:
    """Raw journal INSERTs bypass the writer that marks ambiguous commits."""
    if journal_write:
        from .trino_manager import AMBIGUOUS_TRANSPORT_ERRORS

        return _contains_error(
            exc, (TrinoAmbiguousWriteError, TimeoutError, *AMBIGUOUS_TRANSPORT_ERRORS)
        )
    return _contains_error(exc, (TrinoAmbiguousWriteError,))


@dataclass
class _EditionRun:
    work: wave.TournamentWork
    prepared: wave.PreparedTournament | None = None
    published: bool = False
    first_error: Exception | None = None
    retryable: bool = False
    replay_unsafe: bool = False

    @property
    def key(self) -> EditionKey:
        return self.work.slug, self.work.season_year

    def fail(self, exc: Exception, *, publication: Literal["bronze", "journal"] | None = None) -> None:
        if self.first_error is None:
            self.first_error = exc
        if (
            ambiguous_write(exc, journal_write=publication == "journal")
            or _global_gate(exc)
            or (publication is not None and _contains_error(exc, _INTERRUPTS))
        ):
            self.replay_unsafe = True
        elif not isinstance(exc, wave.WavePlanError):
            self.retryable = True
        logger.error("ESPN %s:%s failed", *self.key, exc_info=exc)

    def outcome(self) -> wave.TournamentOutcome:
        if self.published:
            assert self.prepared is not None
            outcome = self.prepared.outcome
            if self.first_error is None:
                return outcome
            failure = wave.failed_outcome(self.work, self.first_error)
            return replace(outcome, state=wave.RED, first_error=failure.first_error)
        assert self.first_error is not None
        return wave.failed_outcome(self.work, self.first_error)


def _editions(works: Iterable[wave.TournamentWork]) -> list[_EditionRun]:
    seen = set()
    editions = []
    for work in works:
        slug, year = work.slug, work.season_year
        if not isinstance(slug, str) or not slug.strip():
            raise ValueError(f"invalid competition slug {slug!r}")
        if not isinstance(year, (int, str)) or not str(year).strip().isdecimal():
            raise ValueError(f"invalid season year {year!r}")
        key = slug.strip().lower(), int(year)
        if key[1] <= 0:
            raise ValueError(f"invalid season year {year!r}")
        if key in seen:
            raise ValueError(f"duplicate ESPN tournament-season {key!r}")
        seen.add(key)
        editions.append(_EditionRun(replace(work, slug=key[0], season_year=key[1])))
    return editions


def group_works(works: Iterable[wave.TournamentWork]) -> list[list[wave.TournamentWork]]:
    """First fit in descending event count, with input order breaking ties."""
    weighted = [(len(set(e.work.event_ids)), e.work) for e in _editions(works)]
    groups: list[list[wave.TournamentWork]] = []
    loads: list[int] = []
    for size, work in sorted(weighted, key=lambda pair: pair[0], reverse=True):
        for i, group in enumerate(groups):
            if len(group) < MAX_GROUP_EDITIONS and loads[i] + size <= MAX_GROUP_EVENTS:
                group.append(work)
                loads[i] += size
                break
        else:
            groups.append([work])
            loads.append(size)
    return groups


def _scope(scopes, slug_column):
    clauses = []
    params = []
    for (slug, year), ids in scopes.items():
        if ids:
            values = ", ".join(str(int(event_id)) for event_id in sorted(ids))
            clauses.append(f"({slug_column} = ? AND season_year = ? AND event_id IN ({values}))")
            params.extend((slug, year))
    return " OR ".join(clauses), tuple(params)


def _stored_rows(trino, scopes):
    stored = {key: [] for key in scopes}
    where, params = _scope(scopes, "competition_slug")
    if where:
        for row in trino.execute_query(wave._select(where), params):
            slug, year, event_id = row[:3]  # first three columns of wave._select
            key = slug, int(year)
            if key in scopes and int(event_id) in scopes[key]:
                stored[key].append(row)
    return stored


def _journalled(trino, kinds):
    done = {key: {} for key in kinds}
    where, params = _scope(kinds, "slug")
    if where:
        sql = (
            "SELECT DISTINCT slug, season_year, event_id, kind, outcome "
            f"FROM {recheck.RECHECK_TABLE} WHERE {where}"
        )
        for slug, year, event_id, kind, outcome in trino.execute_query(sql, params):
            key, event_id = (slug, int(year)), int(event_id)
            if key in kinds and kinds[key].get(event_id) == kind:
                if done[key].get(event_id) in (None, recheck.FAILED):
                    done[key][event_id] = str(outcome)
    return done


def _previous_state(editions, trino):
    scopes = {e.key: {int(i) for i in e.work.event_ids} for e in editions}
    try:
        stored = _stored_rows(trino, scopes)
    except _INTERRUPTS:
        raise
    except Exception as exc:
        stored = {}
        for edition in editions:
            if scopes[edition.key]:
                edition.fail(exc)

    kinds = {
        e.key: {i: kind for i, kind in e.work.rechecks.items() if i in scopes[e.key]}
        for e in editions if e.first_error is None
    }
    try:
        done = _journalled(trino, kinds)
    except _INTERRUPTS:
        raise
    except Exception as exc:
        done = {}
        for edition in editions:
            if kinds.get(edition.key):
                edition.fail(exc)
    return stored, done


def _prepare(edition, *, client, trino, run_id, stored_rows, done) -> Exception | None:
    """Drain the edition, retain its first error and propagate any transport gate."""
    prepared = None
    gate = None
    try:
        stored = {match.event_id: match for match in map(wave.BronzeMatch.from_row, stored_rows)}
        prepared = wave.prepare_tournament(
            edition.work, client=client, trino=trino, run_id=run_id,
            stored=stored, done=done,
        )
    except _INTERRUPTS:
        raise
    except Exception as exc:
        edition.fail(exc)
        if _global_gate(exc):
            gate = exc
    if prepared is not None and prepared.error is not None:
        edition.fail(prepared.error)
        if _global_gate(prepared.error):
            gate = prepared.error
    try:
        # Failed parses and downloads can also leave raw writes queued.
        client.flush()
        if prepared is not None:
            bronze_writer.preflight_tournament_batch(prepared.batch)
            edition.prepared = prepared
            if not prepared.batch.matches:
                edition.published = True
    except _INTERRUPTS:
        raise
    except Exception as exc:
        edition.fail(exc)
        if gate is None and _global_gate(exc):
            gate = exc
    return gate


def _publish(editions, *, client, trino, conn, child_trino_factory):
    ready = [e for e in editions if e.prepared is not None and e.prepared.batch.matches]
    if not ready:
        return
    try:
        receipts = bronze_writer.write_ready_batches(
            [e.prepared.batch for e in ready], trino=trino,
            state_dir=client_state_dir(client), child_trino_factory=child_trino_factory,
            replace_frame=ready_replace.replace_ready_frame,
        )
    except Exception as exc:
        # Commit state is unknown. Leave the candidates and journal alone.
        for edition in ready:
            edition.fail(exc, publication="bronze")
        return
    journal_editions = []
    journal_rows = []
    for edition in ready:
        if receipts.get(edition.key) is None:
            edition.fail(bronze_writer.BronzeBatchError(f"missing receipt for {edition.key}"))
            continue
        edition.published = True
        if edition.prepared.journal:
            journal_editions.append(edition)
            journal_rows.extend(edition.prepared.journal)
    if journal_rows:
        try:
            recheck.flush_rechecks(conn, journal_rows)
        except Exception as exc:
            for edition in journal_editions:
                edition.fail(exc, publication="journal")


def run_tournament_group(
    works: Sequence[wave.TournamentWork], *, client, trino, conn, run_id: str, task_id: str,
    child_trino_factory=None,
) -> list[wave.TournamentOutcome]:
    """Return outcomes in plan order, carrying partial results on GroupRunError."""
    editions = _editions(works)
    if hasattr(client, "attempt_journal"):
        client.run_id, client.task_id = run_id, task_id
    gate = None
    try:
        stored, done = _previous_state(editions, trino)
        for edition in editions:
            if edition.first_error is not None:
                continue
            if gate is not None:
                edition.fail(gate)
                continue
            gate = _prepare(
                edition, client=client, trino=trino, run_id=run_id,
                stored_rows=stored.get(edition.key, ()), done=done.get(edition.key, {}),
            )
        _publish(
            editions, client=client, trino=trino, conn=conn,
            child_trino_factory=child_trino_factory,
        )
    finally:
        try:
            wave.finish_requests(client, conn, run_id=run_id, task_id=task_id)
        except CancelledError:
            raise
        except Exception as exc:
            if not editions:
                raise
            for edition in editions:
                edition.fail(exc, publication="journal")

    outcomes = [e.outcome() for e in editions]
    if any(e.first_error is not None for e in editions):
        no_retry = any(e.replay_unsafe for e in editions) or not any(e.retryable for e in editions)
        raise GroupRunError(outcomes, no_retry=no_retry)
    return outcomes
