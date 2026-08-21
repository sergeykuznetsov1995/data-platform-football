#!/usr/bin/env python3
"""
FotMob Scraper Runner Script
============================

Standalone runner for source-native FotMob ingestion.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Pure HTTP — FotMob's public ``/api/data`` endpoints require no browser, no
Cloudflare bypass and no cookies.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from scrapers.fotmob.scope_codec import format_scope_token, parse_scope_groups
from scrapers.fotmob.source_refresh import (
    REPLAY_MISSING_INPUT_PROOF_SCHEMA as REPLAY_MISSING_INPUT_SCHEMA,
)
from scrapers.fotmob.player_collector import (
    PLAYER_COLLECTOR_ENTITIES,
    PLAYER_COLLECTOR_MAX_DIRECT_MIB,
    PLAYER_COLLECTOR_MAX_REQUESTS,
    PLAYER_COLLECTOR_MODE,
    PLAYER_COLLECTOR_PLAYER_LIMIT,
    PLAYER_COLLECTOR_PROFILE,
    PLAYER_COLLECTOR_REQUESTS_PER_MINUTE,
    normalize_player_collector_ids,
    player_collector_ids_sha256,
    player_collector_plan_signature,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ``refresh`` is a runner-only mode: it plans and writes exactly like ``daily``
# (current selected/latest seasons of the whole included men's catalog,
# stalest-first) but is meant to run bounded (``--season-limit``) and rotating
# from the continuous refresh DAG, without the pinned 21-competition daily
# contract.  It maps onto ``RunMode.DAILY`` internally so the source planner
# enum stays untouched; the report keeps ``mode="refresh"`` for DAG routing.
NATIVE_MODES = (
    "discover",
    "daily",
    "backfill",
    "replay",
    "refresh",
    PLAYER_COLLECTOR_MODE,
)
NATIVE_ENTITIES = frozenset(
    {"season", "leaderboards", "matches", "teams", "players", "transfers"}
)
DEFAULT_NATIVE_ENTITIES = frozenset({"season", "leaderboards"})
NATIVE_PRIMARY_COUNTS = {
    "competition_catalog": "competitions",
    "competition_seasons": "seasons",
    "season_bundle": "matches",
    "leaderboards": "rows",
    "transfer_events": "events",
    "match_payloads": "rows",
    "team_snapshots": "teams",
    "player_snapshots": "players",
    "scope_completion": "scopes",
    "competition_completion": "competitions",
    "current_views": "views",
}
_MISSING_PLAYER_RAW_ERROR = "no raw-bearing v1/v2 player manifest for offline replay"
_SOURCE_GAP_RETRY_REASON = "source-advertised finished match payloads absent"
_SOURCE_GAP_REASON = (
    "two successful fetches lacked source-advertised finished match payloads"
)

PUBLICATION_BINDING_ARGUMENTS = {
    "schema": "publication_schema",
    "source": "publication_source",
    "owner": "publication_owner",
    "data_interval_start": "publication_data_interval_start",
    "data_interval_end": "publication_data_interval_end",
    "runtime_fingerprint": "publication_runtime_fingerprint",
}
_ACTIVE_PUBLICATION_GENERATION: str | None = None
FOTMOB_SCOPE_JSON_ENV = "FOTMOB_SCOPE_JSON"

# Кулдаун успешно закрытого скоупа текущей полосы. 48 часов означают «турнир
# обойдён, можно не возвращаться», но флаг `finished` матчу проставляет именно
# обход расписания: живой замер интервалов между касаниями текущего сезона даёт
# p50 58,3 ч и p90 152,6 ч, поэтому сыгранный матч ждёт флага двое-трое суток, а
# без флага не качается его карточка (#1193, `include_unfinished=False`). Долг
# «сыграно по времени, но без флага» на 20.08 — 172 матча, из них 165 моложе трёх
# суток, то есть это ровно цена кулдауна.
#
# Поэтому срок выбирается по ОБЯЗАТЕЛЬСТВУ, а не константой: если в расписании
# остались матчи, чьё начало прошло, а терминального статуса нет, скоуп должен
# вернуться на ближайшем ране. Спрос растёт только у тех турниров, у которых
# обязательство реально висит (20.08 это 28-31 из 450), и почти весь прирост —
# дешёвый `not_modified` (1006 из 1554 обращений `league_season` за 7 суток).
#
# Известный хвост: матч, которому источник НИКОГДА не проставит терминальный
# статус (брошенный, снятый не тем полем), держит свой турнир на коротком сроке
# бессрочно. Цена этого — один дешёвый `league_season` на ран, а таких матчей
# живьём два в одном турнире, поэтому потолка на число коротких сроков нет:
# он усложнил бы правило ради случая, которого пока нет.
CURRENT_SCOPE_COOLDOWN = timedelta(hours=48)
CURRENT_SCOPE_OBLIGATION_COOLDOWN = timedelta(hours=2)
# Сколько ждать ПОСЛЕ начала матча, прежде чем идти за его флагом: полтора часа
# игры плюс запас на то, что источник проставляет терминальный статус не мгновенно.
MATCH_SETTLE_MARGIN = timedelta(hours=3)

# Единственность писателя bronze (B7). max_active_runs=1 сериализует только
# DagRun'ы одного дага, а ручной добор и осиротевший скрапер (PPid=1) пишут в те
# же таблицы мимо неё; ретрая на конфликт коммита у FotMob нет. Захват обязан
# быть НЕблокирующим: второй писатель должен падать сразу с внятной ошибкой, а
# не ждать и не писать параллельно. Advisory-замок сессии освобождается сам,
# когда соединение умирает, — поэтому убитый SIGKILL ран не оставляет висящего
# замка, в отличие от записи-владельца в таблице.
WRITER_LOCK_ENV = "FOTMOB_WRITER_LOCK"
_WRITER_LOCK_KEY = int.from_bytes(
    hashlib.blake2b(b"fotmob-bronze-writer", digest_size=8).digest(),
    "big",
    signed=True,
)


def _publication_from_args(args) -> dict[str, Any] | None:
    """Return the canonical generation identity supplied by the owner DAG.

    ``None`` disables the publication machinery for this run: either the
    ceremony is not deployed and no ``--publication-*`` argument was given
    (ad-hoc run), or the owner DAG handed over an explicit ceremony-disabled
    stub.  With a deployed ceremony the arguments stay exactly as mandatory
    as before, and a partial set always fails.
    """

    from utils.fotmob_publication import (
        FOTMOB_PUBLICATION_DISABLED_SCHEMA,
        fotmob_ceremony_configured,
        publication_from_payload,
    )

    values = [args.publication_generation_id] + [
        getattr(args, argument) for argument in PUBLICATION_BINDING_ARGUMENTS.values()
    ]
    ceremony_free = not any(str(value or "").strip() for value in values) or (
        args.publication_schema == FOTMOB_PUBLICATION_DISABLED_SCHEMA
    )
    if ceremony_free and not fotmob_ceremony_configured():
        return None
    return publication_from_payload(
        {
            "generation_id": args.publication_generation_id,
            "binding": {
                field: getattr(args, argument)
                for field, argument in PUBLICATION_BINDING_ARGUMENTS.items()
            },
        }
    )


def _attest_native_runtime(args, publication: Mapping[str, Any]) -> dict[str, Any]:
    """Re-attest mutable bind bytes immediately before the Bronze guard."""

    if (publication.get("binding") or {}).get("owner") == "shared":
        from utils.fotmob_publication import attest_fotmob_shared_runtime

        return attest_fotmob_shared_runtime(require_scheduled_owner=False)
    from utils.fotmob_publication import attest_fotmob_isolated_runtime

    scopes = [
        format_scope_token(competition_id, season)
        for competition_id, season in _parse_scopes(args.scope)
    ]
    source_refresh = getattr(args, "source_refresh_contract", None)
    return attest_fotmob_isolated_runtime(
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        writer_identity={
            "component": "bronze_runner",
            "dag_id": os.environ.get("AIRFLOW_CTX_DAG_ID"),
            "run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
            "mode": args.mode,
            "catalog_contract": args.catalog_contract,
            "scopes": scopes,
            "entities": sorted(_selected_native_entities(args)),
            "competition_limit": args.competition_limit,
            "season_limit": args.season_limit,
            "match_limit": args.match_limit,
            "team_limit": args.team_limit,
            "player_limit": args.player_limit,
            "max_requests": args.max_requests,
            "max_direct_mib": args.max_direct_mib,
            "max_proxy_mib": args.max_proxy_mib,
            "requests_per_minute": args.requests_per_minute,
            "max_attempts": args.max_attempts,
            "deadline": args.deadline,
            "next_build_id": args.next_build_id,
            "source_refresh_profile": (
                source_refresh.get("profile")
                if isinstance(source_refresh, Mapping)
                else None
            ),
            "source_refresh_targets_sha256": (
                source_refresh.get("sha256")
                if isinstance(source_refresh, Mapping)
                else None
            ),
            "source_refresh_target_count": (
                source_refresh.get("target_count")
                if isinstance(source_refresh, Mapping)
                else None
            ),
            "publication": dict(publication),
        },
    )


class WriterLockBusy(RuntimeError):
    """Другой процесс уже держит право записи в bronze FotMob."""


@contextmanager
def _writer_lock(environ: Mapping[str, str] | None = None) -> Iterator[bool]:
    """Взять НЕблокирующий advisory-замок писателя на всё время рана.

    Замок общий для всех входов: и полоса из DAG, и ручной добор берут один и
    тот же ключ, поэтому второй писатель падает сразу, а не пишет параллельно.
    Отключается только явно (``FOTMOB_WRITER_LOCK=0``) — для офлайн-реплея и
    тестов; молчаливого обхода нет, иначе защита превращается в декорацию.
    """

    env = os.environ if environ is None else environ
    if str(env.get(WRITER_LOCK_ENV, "1")).strip().casefold() in {"0", "false", "no"}:
        logger.warning(
            "FotMob writer lock disabled via %s — параллельная запись не защищена",
            WRITER_LOCK_ENV,
        )
        yield False
        return

    import psycopg2

    from scrapers.fbref.control.store import resolve_control_db_uri

    connection = psycopg2.connect(resolve_control_db_uri(env))
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (_WRITER_LOCK_KEY,))
            acquired = bool(cursor.fetchone()[0])
        if not acquired:
            raise WriterLockBusy(
                "another FotMob writer holds the bronze writer lock "
                f"(key {_WRITER_LOCK_KEY}); refusing to write in parallel"
            )
        yield True
    finally:
        # Закрытие соединения снимает замок сессии — отдельный UNLOCK не нужен
        # и был бы хуже: он не отработает при падении процесса.
        connection.close()


@contextmanager
def _native_writer_fence(
    publication: Mapping[str, Any],
) -> Iterator[dict[str, Any]]:
    """Hold and verify the exact active ControlStore writer generation."""

    from scrapers.fbref.control import ControlStore
    from utils.fotmob_publication import FOTMOB_PUBLICATION_SOURCE

    generation_id = publication["generation_id"]
    with ControlStore.from_env().guard_publication_writer(
        generation_id,
        source=FOTMOB_PUBLICATION_SOURCE,
    ) as state:
        violations = []
        if not isinstance(state, Mapping):
            violations.append("ControlStore returned no generation state")
        else:
            if state.get("generation_id") != generation_id:
                violations.append("generation_id mismatch")
            if state.get("source") != FOTMOB_PUBLICATION_SOURCE:
                violations.append("source mismatch")
            if state.get("binding") != publication["binding"]:
                violations.append("binding mismatch")
            if str(state.get("status") or "").casefold() != "running":
                violations.append(f"status={state.get('status')!r}")
            if str(state.get("phase") or "").casefold() != "writing":
                violations.append(f"phase={state.get('phase')!r}")
            if not bool(state.get("active")):
                violations.append("publication lock is inactive")
        if violations:
            raise RuntimeError(
                "FotMob native writer fence rejected run: " + "; ".join(violations)
            )
        global _ACTIVE_PUBLICATION_GENERATION
        if _ACTIVE_PUBLICATION_GENERATION is not None:
            raise RuntimeError("FotMob native writer fence is already active")
        _ACTIVE_PUBLICATION_GENERATION = generation_id
        try:
            yield dict(state)
        finally:
            _ACTIVE_PUBLICATION_GENERATION = None


def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    """Publish a report atomically so validators never observe partial JSON."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _safe_run_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "run"


def _parse_scopes(values: Iterable[str]) -> tuple[tuple[int, str], ...]:
    """Parse repeatable/comma-separated exact ``competition_id=season`` scopes."""

    try:
        groups = tuple(values)
        # The BashOperator hands the optional default through JSON as one
        # empty string. Normalize only that sentinel before the strict codec;
        # an empty item anywhere else is malformed scope input.
        if groups == ("",):
            return ()
        return parse_scope_groups(groups)
    except ValueError as exc:
        raise ValueError(f"invalid --scope: {exc}") from exc


def _scope_groups_from_environment() -> list[str]:
    """Decode the DAG's shell-safe exact scope handoff, if configured."""

    raw = os.environ.get(FOTMOB_SCOPE_JSON_ENV)
    if raw is None:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{FOTMOB_SCOPE_JSON_ENV} must contain JSON") from exc
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError(f"{FOTMOB_SCOPE_JSON_ENV} must contain a string or string list")


def _source_refresh_artifact_path() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "configs/fotmob/issue-930-player-source-refresh.json"
    )


def _load_source_refresh_contract(args) -> dict[str, Any] | None:
    profile = str(getattr(args, "source_refresh_profile", "") or "").strip()
    supplied_sha = (
        str(getattr(args, "source_refresh_targets_sha256", "") or "").strip().casefold()
    )
    if not profile and not supplied_sha:
        return None
    from scrapers.fotmob.source_refresh import (
        PLAYER_SOURCE_REFRESH_PROFILE,
        load_player_source_refresh_contract,
    )

    if profile != PLAYER_SOURCE_REFRESH_PROFILE:
        raise ValueError("unknown FotMob source-refresh profile")
    contract = load_player_source_refresh_contract(_source_refresh_artifact_path())
    if supplied_sha != contract["sha256"]:
        raise ValueError("source-refresh target SHA differs from reviewed artifact")
    return contract


def _selected_native_entities(args) -> frozenset[str]:
    if getattr(args, "source_refresh_contract", None) is not None:
        return frozenset({"players"})
    return _parse_native_entities(args.entities)


def _parse_native_entities(value: str) -> frozenset[str]:
    selected = {
        item.strip().lower() for item in str(value or "").split(",") if item.strip()
    }
    if not selected:
        return DEFAULT_NATIVE_ENTITIES
    unknown = sorted(selected - NATIVE_ENTITIES)
    if unknown:
        raise ValueError(
            f"unknown native entities: {', '.join(unknown)}; expected one of "
            f"{', '.join(sorted(NATIVE_ENTITIES))}"
        )
    if "players" in selected:
        # Player identities come from observed team squads. Fetching players
        # without teams would silently turn the requested entity into a no-op.
        selected.add("teams")
    return frozenset(selected)


def _outstanding_targets(operation) -> int:
    """Return requested targets without an explicit terminal disposition."""

    intentional_not_available = min(
        int(operation.not_available),
        int(operation.metadata.get("intentional_not_available") or 0),
    )
    resolved = (
        int(operation.succeeded) + int(operation.skipped) + intentional_not_available
    )
    return max(0, int(operation.attempted) - resolved)


def _player_replay_missing_raw_ids(operation) -> tuple[int, ...] | None:
    """Return typed missing IDs only when they explain the whole operation."""

    raw_ids = operation.metadata.get("missing_raw_player_ids")
    outcomes = operation.metadata.get("terminal_outcomes")
    if (
        not isinstance(raw_ids, list)
        or not raw_ids
        or any(type(value) is not int or value <= 0 for value in raw_ids)
        or raw_ids != sorted(set(raw_ids))
        or not isinstance(outcomes, list)
        or len(outcomes) != int(operation.attempted)
        or operation.retryable
        or operation.terminal
        or int(operation.skipped) != 0
        or int(operation.review_required) != 0
    ):
        return None
    outcome_by_id: dict[int, str] = {}
    for item in outcomes:
        if (
            not isinstance(item, Mapping)
            or set(item) != {"player_id", "status"}
            or type(item.get("player_id")) is not int
            or int(item["player_id"]) <= 0
            or item["player_id"] in outcome_by_id
            or item.get("status")
            not in {"success", "not_available", "missing_raw_input"}
        ):
            return None
        outcome_by_id[int(item["player_id"])] = str(item["status"])
    missing_ids = tuple(raw_ids)
    if sorted(
        player_id
        for player_id, status in outcome_by_id.items()
        if status == "missing_raw_input"
    ) != list(missing_ids):
        return None
    status_counts = {status: sum(1 for value in outcome_by_id.values() if value == status)
                     for status in ("success", "not_available", "missing_raw_input")}
    if (
        int(operation.succeeded) != status_counts["success"]
        or int(operation.not_available) != status_counts["not_available"]
        or int(operation.metadata.get("intentional_not_available") or 0)
        != status_counts["not_available"]
    ):
        return None
    expected_errors = sorted(
        f"player {player_id}: {_MISSING_PLAYER_RAW_ERROR}" for player_id in missing_ids
    )
    if sorted(operation.errors) != expected_errors:
        return None
    if int(operation.succeeded) + int(operation.not_available) + len(
        missing_ids
    ) != int(operation.attempted):
        return None
    return missing_ids


def _replay_missing_raw_evidence(
    *,
    operations: list[Any],
    work_plan: Any,
    planned_scopes: list[str],
    completed_scopes: list[str],
    gap_entries: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Emit evidence only when missing player raw is the sole replay failure."""

    if not gap_entries:
        return None
    gap_scopes = [str(entry["scope"]) for entry in gap_entries]
    if (
        len(gap_scopes) != len(set(gap_scopes))
        or len(planned_scopes) != len(set(planned_scopes))
        or len(completed_scopes) != len(set(completed_scopes))
        or set(completed_scopes).intersection(gap_scopes)
        or set(completed_scopes).union(gap_scopes) != set(planned_scopes)
        or len(completed_scopes) + len(gap_scopes) != len(planned_scopes)
        or int(work_plan.attempted) != len(planned_scopes)
        or int(work_plan.succeeded) != len(completed_scopes)
        or int(work_plan.skipped) != 0
        or work_plan.errors
        or work_plan.terminal
        or work_plan.retryable
        != [str(entry["work_plan_retryable"]) for entry in gap_entries]
        or work_plan.metadata.get("incomplete_scopes")
        != [
            {"scope": entry["scope"], "outstanding": entry["outstanding"]}
            for entry in gap_entries
        ]
    ):
        return None

    allowed_failures = {id(work_plan)}
    missing_player_ids: set[int] = set()
    for entry in gap_entries:
        operation = entry["player_operation"]
        typed_ids = _player_replay_missing_raw_ids(operation)
        if typed_ids is None or list(typed_ids) != entry["missing_player_ids"]:
            return None
        allowed_failures.add(id(operation))
        missing_player_ids.update(typed_ids)
    if any(
        not operation.ok and id(operation) not in allowed_failures
        for operation in operations
    ):
        return None
    return {
        "schema": REPLAY_MISSING_INPUT_SCHEMA,
        "failure_class": "missing_player_raw_inputs_only",
        "missing_player_ids": sorted(missing_player_ids),
        "affected_scopes": sorted(gap_scopes),
    }


def _identity_hash(values: Iterable[Any]) -> str:
    material = "\0".join(sorted(str(value) for value in values)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _catalog_decision_payload(evidence) -> dict[str, Any]:
    payload = asdict(evidence)
    for field in ("probe_status", "decision"):
        value = payload.get(field)
        payload[field] = getattr(value, "value", value)
    for field in ("next_probe_at", "observed_at"):
        value = payload.get(field)
        payload[field] = value.isoformat() if isinstance(value, datetime) else value
    return payload


def _match_kickoff(value: Any) -> datetime | None:
    """Время начала матча источника как наивный UTC, либо None."""

    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _match_is_settled(match: Mapping[str, Any]) -> bool:
    """Матч, за которым больше не надо возвращаться.

    `postponed` и `awarded` парсер сохраняет наравне с `finished`/`cancelled`
    (`scrapers/fotmob/parsers.py:143-146`), и без них прошедший перенесённый или
    присуждённый матч навсегда остаётся «обязательством»: скоуп получал бы
    короткий срок бесконечно.

    Про `awarded` — замер 20.08 по `bronze.fotmob_matches_current`: присуждённых
    матчей 169, и у ВСЕХ 169 стоит ещё и `finished`, то есть сегодня такой матч
    и без этой ветки закрывается по `finished`. Статус добавлен не как лечение
    живого зависания, а потому что порядок выставления флагов у источника нам не
    подконтролен: присуждение раньше отметки о завершении дало бы вечный
    двухчасовой опрос.
    """

    return bool(
        match.get("finished")
        or match.get("cancelled")
        or match.get("postponed")
        or match.get("awarded")
    )


def _schedule_cooldown(matches: Iterable[Mapping[str, Any]], now: datetime) -> timedelta:
    """Через сколько вернуться к расписанию сезона.

    Считается по СВЕЖЕРАЗОБРАННОМУ расписанию, а не по числу недокачанных
    карточек: `source_missing_matches` входит в само условие успеха скоупа
    (`scope_ok`), поэтому в ветке успеха он тождественно ноль, и признак оттуда
    был бы всегда ложным — правка получилась бы «зелёной, но пустой».

    Три случая, и средний — главный:

    * матч уже начался и не закрыт — обязательство висит прямо сейчас,
      возвращаемся ближайшим раном;
    * ближайший матч ещё впереди — возвращаемся вскоре ПОСЛЕ него. Только это
      и лечит корень #1193: обход, случившийся утром, видит вечерний матч ещё
      будущим, и при фиксированных 48 часах никто не пришёл бы за его флагом
      двое суток;
    * впереди ничего нет — обычный долгий срок.

    Матч без разбираемого времени начала обязательством не считается: иначе
    мусорное поле держало бы скоуп на коротком сроке вечно.
    """

    earliest_future: datetime | None = None
    for match in matches:
        if _match_is_settled(match):
            continue
        kickoff = _match_kickoff(match.get("utc_time"))
        if kickoff is None:
            continue
        if kickoff <= now:
            return CURRENT_SCOPE_OBLIGATION_COOLDOWN
        if earliest_future is None or kickoff < earliest_future:
            earliest_future = kickoff
    if earliest_future is None:
        return CURRENT_SCOPE_COOLDOWN
    # Пол здесь не нужен: у будущего матча `kickoff > now`, поэтому срок заведомо
    # больше запаса на доигрывание.
    return min(CURRENT_SCOPE_COOLDOWN, earliest_future + MATCH_SETTLE_MARGIN - now)


def _scope_attempt_payload(state, *, plan_signature: str | None = None) -> dict[str, Any]:
    # Отчёт — доказательство о КОНТРАКТЕ, поэтому наружу идёт контрактная
    # подпись рана; журнальная подпись, под которой состояние лежит в манифесте,
    # остаётся внутренней деталью планировщика.
    return {
        "competition_id": state.competition_id,
        "source_season_key": state.source_season_key,
        "plan_signature": plan_signature or state.plan_signature,
        "attempt_count": state.attempt_count,
        "last_attempt_at": state.last_attempt_at.replace(
            tzinfo=timezone.utc
        ).isoformat(),
        "next_retry_at": (
            state.next_retry_at.replace(tzinfo=timezone.utc).isoformat()
            if state.next_retry_at is not None
            else None
        ),
        "outcome": state.outcome,
        "reason": state.reason,
        "attempt_identities": list(state.attempt_identities),
    }


def _scope_retry_due(attempt_count: int, observed_at: datetime) -> datetime:
    delays = (timedelta(minutes=15), timedelta(hours=1), timedelta(hours=6), timedelta(hours=24))
    return observed_at + delays[min(max(int(attempt_count), 1), len(delays)) - 1]


def _is_budget_deferral_error(value: Any) -> bool:
    text = str(value).casefold()
    return "budget" in text and ("request" in text or "byte" in text)


def _native_output_payload(report) -> dict[str, Any]:
    payload = report.as_dict()
    tables: list[str] = []
    rows: dict[str, int] = {}
    errors: list[str] = []
    for operation in report.operations:
        tables.extend(operation.tables)
        primary_key = NATIVE_PRIMARY_COUNTS.get(operation.entity)
        primary_count = (
            operation.counts.get(primary_key, operation.succeeded)
            if primary_key
            else operation.succeeded
        )
        rows[operation.entity] = rows.get(operation.entity, 0) + int(primary_count)
        errors.extend(operation.errors)
        errors.extend(f"retryable: {item}" for item in operation.retryable)
        errors.extend(f"terminal: {item}" for item in operation.terminal)
    payload.update(
        {
            # Additive compatibility for existing validators/report readers.
            "tables": list(dict.fromkeys(tables)),
            "rows": rows,
            "errors": errors,
            "complete": report.status == "success",
        }
    )
    return payload


# Buffered repository of the running native service. finish() is the normal
# flush point, but an exception escaping _run_native (or the driver's SIGTERM
# at unit timeout) would otherwise drop up to batch_size-1 already-paid-for
# targets; main() salvage-flushes through this handle before reporting failure.
_ACTIVE_NATIVE_SERVICE = None


def _deactivate_native_service(service=None) -> None:
    """Clear the signal/salvage handle without clobbering a newer run."""

    global _ACTIVE_NATIVE_SERVICE
    if service is None or _ACTIVE_NATIVE_SERVICE is service:
        _ACTIVE_NATIVE_SERVICE = None


def _salvage_flush() -> None:
    """Best-effort durability for buffered commits on an abnormal exit."""

    service = _ACTIVE_NATIVE_SERVICE
    if service is None:
        return
    try:
        tables = service.repository.flush()
        if tables:
            logger.warning("salvage flush persisted buffered commits: %s", tables)
    except Exception:
        logger.exception("salvage flush after runner failure also failed")


def _build_native_service(args, run_id: str):
    """Construct production dependencies lazily for fast CLI/unit imports."""

    from scrapers.fotmob.planner import RunMode, TransportBudget
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.repository import FotMobRepository
    from scrapers.fotmob.service import FotMobIngestService
    from scrapers.fotmob.transport import FotMobTransport
    from scrapers.utils.rate_limiter import RateLimiter

    raw_store = (
        FotMobRawStore.from_uri(args.raw_store_uri)
        if args.raw_store_uri
        else FotMobRawStore.from_env(optional=False)
    )
    limiter = RateLimiter(
        max_requests=args.requests_per_minute,
        window_seconds=60,
        # Avoid an initial 30-request burst when several workers start.
        burst_size=max(1, args.workers),
    )
    transport = FotMobTransport(
        raw_store=raw_store,
        max_attempts=args.max_attempts,
        # systemd sends KILL 30s after TERM.  A worker already inside requests
        # must return early enough for buffer reconciliation/reporting.
        timeout=(5.0, 20.0),
        rate_limiter=limiter,
    )
    repository = FotMobRepository(
        batch_size=args.commit_batch_size,
        max_buffered_rows=args.max_buffered_rows,
    )
    budget = TransportBudget(
        max_requests=args.max_requests,
        max_direct_bytes=int(args.max_direct_mib * 1024 * 1024),
        max_proxy_bytes=int(args.max_proxy_mib * 1024 * 1024),
    )
    service = FotMobIngestService(
        transport=transport,
        repository=repository,
        # ``refresh`` reuses DAILY service semantics (current-season focus,
        # 1-year transfer window).  See NATIVE_MODES.
        mode=RunMode(
            "daily"
            if args.mode in {"refresh", PLAYER_COLLECTOR_MODE}
            else args.mode
        ),
        budget=budget,
        run_id=run_id,
        max_workers=args.workers,
    )
    global _ACTIVE_NATIVE_SERVICE
    _ACTIVE_NATIVE_SERVICE = service
    return service, raw_store


def _run_native(args, *, service=None, raw_store=None) -> tuple[int, dict[str, Any]]:
    """Run source-native discovery and a deterministic, budgeted work plan."""

    # An empty publication id means a ceremony-free run: no guard is active
    # and none is required.
    if _ACTIVE_PUBLICATION_GENERATION != (
        getattr(args, "publication_generation_id", None) or None
    ):
        raise RuntimeError(
            "FotMob native service construction requires its exact active "
            "publication writer guard"
        )

    from scrapers.fotmob.planner import (
        MANDATORY_COMPETITION_IDS,
        RunMode,
        ScopeLane,
        deterministic_plan_signature,
        plan_seasons,
    )
    from scrapers.fotmob.catalog import CLASSIFIER_VERSION
    from scrapers.fotmob.catalog_contract import build_catalog_contract
    from scrapers.fotmob.repository import (
        PARSER_VERSION,
        deterministic_target_batch_id,
    )
    from scrapers.fotmob.service import OperationResult
    from scrapers.fotmob.transport import canonicalize_target

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = args.run_id or (f"fotmob-{datetime.now(timezone.utc):%Y%m%dT%H%M%S.%fZ}")
    if service is None:
        service, raw_store = _build_native_service(args, run_id)
    global _ACTIVE_NATIVE_SERVICE
    _ACTIVE_NATIVE_SERVICE = service
    operations = []
    automatic_catalog = (
        getattr(args, "catalog_contract", "") == "fotmob-catalog-v1"
    )
    automatic_contract = None
    automatic_lane = None
    catalog_ids_evidence: list[int] = []
    catalog_decisions_evidence: list[dict[str, Any]] = []
    automatic_deferrals: list[dict[str, Any]] = []

    def add_automatic_deferral(
        *, kind: str, target_type: str, targets: Iterable[Any], reason: str
    ) -> None:
        if not automatic_catalog:
            return
        normalized_targets = list(targets)
        if not normalized_targets:
            return
        automatic_deferrals.append(
            {
                "kind": kind,
                "target_type": target_type,
                "targets": normalized_targets,
                "reason": reason,
            }
        )

    def finish() -> tuple[int, dict[str, Any]]:
        # Buffered commits are only durable once flushed. Every exit path of
        # this run — completion, budget cut, empty catalog — goes through
        # finish(), so the flush belongs here and its failure must turn the
        # run red instead of silently dropping targets.
        flush_operation = OperationResult("commit_flush", attempted=1)
        try:
            flush_operation.tables.extend(service.repository.flush())
            flush_operation.succeeded = 1
        except Exception as exc:
            flush_operation.errors.append(f"commit flush: {type(exc).__name__}: {exc}")
        operations.append(flush_operation)

        view_operation = OperationResult("current_views", attempted=1)
        try:
            views = service.repository.ensure_current_views()
            view_operation.succeeded = 1
            view_operation.tables.extend(views)
            view_operation.counts["views"] = len(views)
        except Exception as exc:
            view_operation.errors.append(
                f"current view refresh: {type(exc).__name__}: {exc}"
            )
        operations.append(view_operation)
        report = service.report(operations, started_at)
        payload = _native_output_payload(report)
        # ``refresh`` runs on DAILY service semantics, so the report would echo
        # "daily"; restore the caller's mode so the DAG routes it to the
        # best-effort (non-daily-contract) validation path.
        payload["mode"] = args.mode
        _deactivate_native_service(service)
        return (0 if report.ok else 1), payload

    source_refresh = getattr(args, "source_refresh_contract", None)
    if args.mode == PLAYER_COLLECTOR_MODE:
        raw_player_ids = service.repository.missing_current_squad_player_ids(
            args.player_limit
        )
        player_ids = list(normalize_player_collector_ids(raw_player_ids))
        if player_ids != raw_player_ids or len(player_ids) > args.player_limit:
            raise RuntimeError("player collector repository selection is not canonical")
        player_operation = service.sync_player_snapshots(
            player_ids,
            # A manifest success without its typed snapshot is exactly the gap
            # this collector repairs. The anti-join prevents refreshing any
            # identity that already has a card row.
            force_refresh=True,
            repair_missing_snapshot=True,
            capture_terminal_outcomes=True,
        )
        operations.append(player_operation)
        raw_outcomes = player_operation.metadata.get("terminal_outcomes")
        valid_outcomes = (
            isinstance(raw_outcomes, list)
            and len(raw_outcomes) == len(player_ids)
            and all(
                isinstance(item, Mapping)
                and set(item) == {"player_id", "status"}
                and type(item.get("player_id")) is int
                and item.get("status") in {"success", "not_available"}
                for item in raw_outcomes
            )
        )
        target_outcomes = list(raw_outcomes) if valid_outcomes else []
        outcome_ids = (
            [int(item["player_id"]) for item in target_outcomes]
            if valid_outcomes
            else []
        )
        contract_operation = OperationResult(
            "player_collector_contract",
            attempted=len(player_ids),
            metadata={
                "profile": PLAYER_COLLECTOR_PROFILE,
                "player_ids_sha256": player_collector_ids_sha256(player_ids),
                "target_outcomes": target_outcomes,
            },
        )
        unavailable = sum(
            item["status"] == "not_available" for item in target_outcomes
        )
        if (
            not valid_outcomes
            or outcome_ids != player_ids
            or player_operation.attempted != len(player_ids)
            or player_operation.succeeded + player_operation.not_available
            != len(player_ids)
            or player_operation.not_available != unavailable
            or player_operation.skipped != 0
            or player_operation.metadata.get("typed_snapshot_writes")
            != player_operation.succeeded
        ):
            contract_operation.errors.append(
                "player collector did not produce one terminal outcome per target"
            )
        else:
            contract_operation.succeeded = len(player_ids)
            contract_operation.counts["terminal_targets"] = len(player_ids)
        operations.append(contract_operation)
        rc, payload = finish()
        payload["selection"] = {
            "profile": PLAYER_COLLECTOR_PROFILE,
            "entities": list(PLAYER_COLLECTOR_ENTITIES),
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "planned_scopes": [],
            "completed_scopes": [],
            "completed_transfer_competition_ids": [],
            "requests_per_minute": args.requests_per_minute,
            "scope_plan_signature": player_collector_plan_signature(player_ids),
            "player_collector": {
                "profile": PLAYER_COLLECTOR_PROFILE,
                "player_ids": player_ids,
                "player_count": len(player_ids),
                "player_ids_sha256": player_collector_ids_sha256(player_ids),
                "player_limit": args.player_limit,
            },
            "target_outcomes": target_outcomes,
        }
        return rc, payload

    if isinstance(source_refresh, Mapping):
        player_operation = service.sync_player_snapshots(
            source_refresh["player_ids"],
            # A task retry may follow partial durable commits. Force the same
            # immutable seven identities to be observed again so retries stay
            # idempotent instead of turning fresh successes into skips.
            force_refresh=True,
            capture_terminal_outcomes=True,
        )
        operations.append(player_operation)

        raw_outcomes = player_operation.metadata.get("terminal_outcomes")
        valid_outcome_shape = (
            isinstance(raw_outcomes, list)
            and len(raw_outcomes) == source_refresh["target_count"]
            and all(
                isinstance(item, Mapping)
                and set(item) == {"player_id", "status"}
                and type(item.get("player_id")) is int
                and isinstance(item.get("status"), str)
                for item in raw_outcomes
            )
        )
        outcome_by_player = (
            {int(item["player_id"]): str(item["status"]) for item in raw_outcomes}
            if valid_outcome_shape
            else {}
        )
        target_outcomes = [
            {
                **target,
                "status": outcome_by_player.get(target["player_id"], "missing"),
            }
            for target in source_refresh["targets"]
        ]
        profile_validation = OperationResult(
            "player_source_refresh_contract",
            attempted=source_refresh["target_count"],
            metadata={
                "profile": source_refresh["profile"],
                "targets_sha256": source_refresh["sha256"],
                "target_outcomes": target_outcomes,
            },
        )
        accepted = {"success", "not_available"}
        intentional_not_available = int(
            player_operation.metadata.get("intentional_not_available") or 0
        )
        reported_not_available = sum(
            item["status"] == "not_available" for item in target_outcomes
        )
        if (
            not valid_outcome_shape
            or player_operation.attempted != source_refresh["target_count"]
            or len(outcome_by_player) != source_refresh["target_count"]
            or set(outcome_by_player) != set(source_refresh["player_ids"])
            or any(item["status"] not in accepted for item in target_outcomes)
            or player_operation.skipped != 0
            or player_operation.not_available != reported_not_available
            or intentional_not_available != reported_not_available
        ):
            profile_validation.errors.append(
                "source refresh did not produce exactly seven terminal outcomes"
            )
        else:
            profile_validation.succeeded = source_refresh["target_count"]
            profile_validation.counts["terminal_targets"] = source_refresh[
                "target_count"
            ]
        operations.append(profile_validation)

        rc, payload = finish()
        payload["selection"] = {
            "profile": source_refresh["profile"],
            "entities": ["players"],
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "scope_plan_signature": source_refresh["plan_signature"],
            "planned_scopes": [],
            "completed_scopes": [],
            "completed_transfer_competition_ids": [],
            "requests_per_minute": args.requests_per_minute,
            "source_refresh": {
                key: source_refresh[key]
                for key in (
                    "profile",
                    "artifact",
                    "sha256",
                    "target_count",
                    "targets",
                    "plan_signature",
                )
            },
            "target_outcomes": target_outcomes,
        }
        return rc, payload

    explicit_scopes = _parse_scopes(args.scope)
    explicit_ids = {competition_id for competition_id, _ in explicit_scopes}
    daily_competition_ids = {
        int(value) for value in getattr(args, "daily_competition_ids", ())
    }
    requested_competition_ids = explicit_ids | daily_competition_ids
    entities = _selected_native_entities(args)

    catalog = service.discover_catalog()
    operations.append(catalog.operation)
    if catalog.discovery is None:
        _, payload = finish()
        return 1, payload

    classifications = list(catalog.classifications)
    catalog_ids = {item.competition.competition_id for item in classifications}
    scope_validation = OperationResult("scope_validation")
    if automatic_catalog:
        catalog_ids_evidence = sorted(catalog_ids)
        evidence_by_id = dict(getattr(catalog, "evidence", {}) or {})
        missing_evidence = sorted(catalog_ids - set(evidence_by_id))
        if missing_evidence:
            scope_validation.errors.append(
                "automatic catalog IDs lack durable profile evidence: "
                + ",".join(map(str, missing_evidence))
            )
        catalog_decisions_evidence = [
            _catalog_decision_payload(evidence_by_id[competition_id])
            for competition_id in catalog_ids_evidence
            if competition_id in evidence_by_id
        ]
    unknown_ids = sorted(requested_competition_ids - catalog_ids)
    if unknown_ids:
        scope_validation.errors.append(
            "requested competition IDs are absent from allLeagues: "
            + ",".join(map(str, unknown_ids))
        )
    by_id = {item.competition.competition_id: item for item in classifications}
    for competition_id in sorted(requested_competition_ids & catalog_ids):
        classification = by_id[competition_id]
        if classification.decision.value != "included":
            scope_validation.errors.append(
                f"requested competition {competition_id} is "
                f"{classification.decision.value}: {classification.reason}"
            )
    if scope_validation.errors:
        operations.append(scope_validation)

    candidates = [
        item
        for item in classifications
        if (
            (not explicit_ids or item.competition.competition_id in explicit_ids)
            and (
                not daily_competition_ids
                or item.competition.competition_id in daily_competition_ids
            )
            and item.decision.value == "included"
        )
    ]
    if automatic_catalog:
        discovery_times: dict[int, datetime] = {}
        latest_entity_attempt = getattr(
            service.repository, "latest_entity_attempt", None
        )
        for item in candidates:
            previous = (
                latest_entity_attempt(
                    "competition_seasons", item.competition.competition_id
                )
                if latest_entity_attempt is not None
                else None
            )
            raw_completed_at = previous.get("completed_at") if previous else None
            try:
                completed_at = (
                    raw_completed_at
                    if isinstance(raw_completed_at, datetime)
                    else datetime.fromisoformat(str(raw_completed_at))
                )
                if completed_at.tzinfo is not None:
                    completed_at = completed_at.astimezone(timezone.utc).replace(
                        tzinfo=None
                    )
            except (TypeError, ValueError):
                completed_at = datetime.min
            discovery_times[item.competition.competition_id] = completed_at
        candidates.sort(
            key=lambda item: (
                discovery_times[item.competition.competition_id],
                item.competition.competition_id,
            )
        )
    else:
        candidates.sort(
            key=lambda item: (
                item.competition.competition_id not in MANDATORY_COMPETITION_IDS,
                item.competition.competition_id,
            )
        )
    all_candidates = list(candidates)
    discovery_plan = OperationResult("competition_discovery_plan")
    if args.competition_limit:
        deferred = candidates[args.competition_limit :]
        candidates = candidates[: args.competition_limit]
        discovery_plan.skipped = len(deferred)
        discovery_plan.metadata["deferred_competition_ids"] = [
            item.competition.competition_id for item in deferred
        ]
        add_automatic_deferral(
            kind="budget",
            target_type="competition_discovery",
            targets=discovery_plan.metadata["deferred_competition_ids"],
            reason="configured competition budget deferred discovery",
        )
    discovery_plan.counts["planned_competitions"] = len(candidates)
    operations.append(discovery_plan)

    seasons = []
    selected_fetches = {}
    max_attempts = max(1, int(getattr(service.transport, "max_attempts", 1)))
    discovery_capacity = service.ledger.remaining_requests // max_attempts
    budget_deferred = candidates[discovery_capacity:]
    candidates = candidates[:discovery_capacity]
    if budget_deferred:
        discovery_plan.skipped += len(budget_deferred)
        discovery_plan.retryable.append(
            f"request budget deferred {len(budget_deferred)} competitions"
        )
        discovery_plan.metadata["budget_deferred_competition_ids"] = [
            item.competition.competition_id for item in budget_deferred
        ]
        add_automatic_deferral(
            kind="budget",
            target_type="competition_discovery",
            targets=discovery_plan.metadata["budget_deferred_competition_ids"],
            reason="request budget deferred competition discovery",
        )
    candidate_profile_payloads = {
        item.competition.competition_id: catalog.profile_payloads[
            item.competition.competition_id
        ]
        for item in candidates
        if item.competition.competition_id in catalog.profile_payloads
    }
    discovery_results = service.discover_competitions(
        candidates,
        profile_payloads=candidate_profile_payloads,
    )
    revalidated_by_id = {
        discovered.competition.competition_id: discovered.classification
        for discovered in discovery_results
    }
    for discovered in discovery_results:
        operations.append(discovered.operation)
        seasons.extend(discovered.seasons)
        if discovered.selected_bundle is not None and discovered.fetch is not None:
            selected_fetches[discovered.selected_bundle.scope.identity[:2]] = (
                discovered.fetch
            )

    if revalidated_by_id:
        classifications = [
            revalidated_by_id.get(
                item.competition.competition_id, item
            )
            for item in classifications
        ]
        all_candidates = [
            revalidated_by_id.get(
                item.competition.competition_id, item
            )
            for item in all_candidates
            if revalidated_by_id.get(
                item.competition.competition_id, item
            ).decision.value
            == "included"
        ]
        for competition_id in sorted(requested_competition_ids & catalog_ids):
            revalidated = revalidated_by_id.get(competition_id)
            if revalidated is None or revalidated.decision.value == "included":
                continue
            scope_validation.errors.append(
                f"requested competition {competition_id} became "
                f"{revalidated.decision.value} after fresh profile validation: "
                f"{revalidated.reason}"
            )
        if scope_validation.errors and scope_validation not in operations:
            operations.append(scope_validation)

    if automatic_catalog:
        evidence_by_id = service.repository.latest_scope_evidence(catalog_ids)
        missing_evidence = sorted(catalog_ids - set(evidence_by_id))
        if missing_evidence:
            scope_validation.errors.append(
                "automatic catalog IDs lack durable post-discovery profile evidence: "
                + ",".join(map(str, missing_evidence))
            )
            if scope_validation not in operations:
                operations.append(scope_validation)
        catalog_decisions_evidence = [
            _catalog_decision_payload(evidence_by_id[competition_id])
            for competition_id in catalog_ids_evidence
            if competition_id in evidence_by_id
        ]
    freshly_validated_included_ids = {
        int(competition_id) for competition_id in catalog.profile_payloads
    }
    freshly_validated_included_ids.update(
        competition_id
        for competition_id, classification in revalidated_by_id.items()
        if classification.decision.value == "included"
    )

    discovered_identities = {season.identity for season in seasons}
    missing_scopes = sorted(set(explicit_scopes) - discovered_identities)
    if missing_scopes:
        # FotMob simply does not advertise every requested ID=season pair (a
        # competition that did not run that season). That is a property of the
        # source, not a run failure: turning it into an error made a whole
        # backfill chunk red even when every other requested scope was
        # collected. Unadvertised pairs are therefore reported as skipped, and
        # only a request where NOTHING asked for exists stays an error.
        scope_validation.skipped += len(missing_scopes)
        scope_validation.metadata["unadvertised_scopes"] = [
            format_scope_token(comp, season) for comp, season in missing_scopes
        ]
        if not (set(explicit_scopes) & discovered_identities):
            scope_validation.errors.append(
                "requested exact scopes were not advertised by FotMob: "
                + ",".join(
                    format_scope_token(comp, season)
                    for comp, season in missing_scopes
                )
            )
        if scope_validation not in operations:
            operations.append(scope_validation)

    mode = RunMode("daily" if args.mode == "refresh" else args.mode)
    scope_entities = frozenset({"season", *(entities - {"transfers"})})
    contract_entities = frozenset({*scope_entities, *(entities & {"transfers"})})
    transfer_window = "1year" if mode == RunMode.DAILY else "all"
    scope_policy = {
        "match_policy": "finished_only",
        "leaderboard_policy": "all_advertised",
        "team_policy": "global_observed_snapshot",
        "player_policy": "global_observed_snapshot",
    }
    if "transfers" in entities:
        scope_policy["transfer_policy"] = {
            "window": transfer_window,
            "pagination": "unique_hits",
            "completion_scope": "included_ids",
            "completion_signature": "catalog_contract",
        }
    automatic_lane = (
        ScopeLane.HISTORY
        if mode == RunMode.BACKFILL
        else ScopeLane.CURRENT
    )
    if automatic_catalog:
        contract_items = (
            []
            if mode == RunMode.DISCOVER
            else plan_seasons(
                classifications,
                seasons,
                mode=mode,
                lane=automatic_lane,
            )
        )
        if (
            catalog.fetch is None
            or not catalog.fetch.content_hash
            or not catalog.fetch.target_key
        ):
            scope_validation.errors.append(
                "automatic catalog contract lacks immutable allLeagues identity"
            )
        else:
            automatic_contract = build_catalog_contract(
                catalog_batch_id=deterministic_target_batch_id(
                    catalog.fetch.target_key,
                    catalog.fetch.content_hash,
                    PARSER_VERSION,
                    getattr(service, "run_id", run_id),
                ),
                catalog_content_hash=catalog.fetch.content_hash,
                classifier_version=CLASSIFIER_VERSION,
                parser_version=PARSER_VERSION,
                entities=contract_entities,
                entity_policy=scope_policy,
                included_ids=[
                    item.competition.competition_id
                    for item in classifications
                    if item.decision.value == "included"
                ],
                scopes=[item.identity for item in contract_items],
            )
        if scope_validation.errors and scope_validation not in operations:
            operations.append(scope_validation)
    # Две подписи с разными задачами.
    #
    # Контрактная (контентная) хеширует состав каталога и потому меняется, как
    # только источник открыл новый сезон или турнир. Как доказательство «этот
    # ран отвечал ровно за этот каталог» она верна и остаётся в отчёте.
    #
    # Журнальная зависит только от набора сущностей и политики полосы, то есть
    # стабильна между ранами. Ключом журнала обязана быть именно она: под
    # контентной подписью история попыток обнулялась каждый ран, планировщик
    # видел пустую карту, все скоупы получали last_attempt=datetime.min и обход
    # вечно начинался с головы очереди — хвост каталога не собирался никогда,
    # а раны при этом были зелёными.
    #
    # Полоса входит в журнальную подпись явно. Сегодня `current` и `history`
    # расходятся по ней случайно — через окно трансферов (`1year` против
    # `all`); стоит убрать `transfers` из состава полосы, и журналы молча
    # сольются, а `plan_seasons` перестанет планировать историей сезоны,
    # закрытые текущей полосой. Для неавтоматического режима формула не
    # меняется: её сверяют приёмка replay и regex публикации.
    journal_plan_signature = deterministic_plan_signature(
        contract_entities,
        policy=(
            {**scope_policy, "scope_lane": automatic_lane.value}
            if automatic_catalog
            else scope_policy
        ),
    )
    scope_plan_signature = (
        automatic_contract.plan_signature
        if automatic_contract is not None
        else journal_plan_signature
    )

    if args.mode == RunMode.DISCOVER.value:
        rc, payload = finish()
        if automatic_contract is not None:
            payload["selection"] = {
                "entities": list(automatic_contract.entities),
                "explicit_scopes": [],
                "competition_limit": 0,
                "season_limit": args.season_limit,
                "scope_plan_signature": scope_plan_signature,
                "planned_scopes": [],
                "completed_scopes": [],
                "completed_transfer_competition_ids": [],
                "requests_per_minute": args.requests_per_minute,
                "scope_lane": automatic_lane.value,
                "catalog_contract": automatic_contract.as_dict(),
                "catalog_ids": catalog_ids_evidence,
                "catalog_decisions": catalog_decisions_evidence,
                "scope_attempts": [],
                "transfer_plan_signature": (
                    scope_plan_signature if "transfers" in entities else None
                ),
                "deferrals": automatic_deferrals,
            }
        return rc, payload
    previously_complete: set[tuple[int, str]] = set()
    if mode == RunMode.BACKFILL:
        # No run_id filter on purpose: service.run_id is the publication
        # generation_id, unique per DagRun, so scoping the resume set to it made
        # every @continuous drain iteration replan the very first chunk forever.
        # The drain resumes across DagRuns on plan_signature + parser_version;
        # a parser bump still reprocesses everything.
        previously_complete = service.repository.completed_scope_keys(
            journal_plan_signature,
        )
    elif mode == RunMode.REPLAY:
        for season in seasons:
            target = canonicalize_target(
                "leagues",
                {
                    "id": season.competition_id,
                    "season": season.source_season_key,
                },
            )
            if raw_store is not None and raw_store.has_target(target):
                previously_complete.add(season.identity)

    attempt_states = (
        service.repository.scope_attempt_states(journal_plan_signature)
        if automatic_catalog and automatic_contract is not None
        else {}
    )

    work = plan_seasons(
        classifications,
        seasons,
        mode=mode,
        previously_successful=previously_complete,
        explicit_scopes=(explicit_scopes or None),
        lane=(automatic_lane if automatic_catalog else None),
        attempt_states=attempt_states,
    )
    daily_scope_times = {}
    if mode == RunMode.DAILY and not automatic_catalog:
        daily_scope_times = service.repository.scope_completion_times(
            journal_plan_signature
        )
        work.sort(
            key=lambda work_item: (
                daily_scope_times.get(work_item.identity, datetime.min),
                work_item.priority,
                work_item.competition_id,
            )
        )
    work_plan = OperationResult(
        "season_work_plan",
        attempted=len(work),
        metadata={
            "scope_plan_signature": scope_plan_signature,
            # Ключ, по которому лежит история попыток в манифесте: без него
            # оператор не может ни проверить прогресс обхода запросом, ни
            # отличить «журнал переехал» от «журнал пуст».
            "journal_plan_signature": journal_plan_signature,
            "scope_entities": sorted(scope_entities),
            "already_complete_scopes": len(previously_complete),
            "attempt_states": len(attempt_states),
            "daily_completion_timestamps": len(daily_scope_times),
        },
    )
    if (
        not work
        and mode in {RunMode.DAILY, RunMode.REPLAY}
        and not automatic_catalog
    ):
        work_plan.errors.append(
            f"{mode.value} discovered no eligible exact season targets"
        )
    if mode == RunMode.DAILY and daily_competition_ids:
        planned_competition_ids = {item.competition_id for item in work}
        missing_current_ids = sorted(daily_competition_ids - planned_competition_ids)
        if missing_current_ids:
            work_plan.errors.append(
                "daily cohort has no selected/latest season for competition IDs: "
                + ",".join(map(str, missing_current_ids))
            )
    planned_scopes = [
        format_scope_token(item.competition_id, item.source_season_key)
        for item in work
    ]
    attempt_operation = OperationResult("scope_attempts")

    def record_automatic_attempt(
        item,
        *,
        outcome: str,
        reason: str,
        next_retry_at: datetime | None = None,
        attempt_identities: tuple[str, ...] = (),
    ) -> None:
        if not automatic_catalog or automatic_contract is None:
            return
        attempt_operation.attempted += 1
        try:
            state = service.record_scope_attempt(
                item.competition_id,
                item.source_season_key,
                plan_signature=journal_plan_signature,
                outcome=outcome,
                reason=reason,
                next_retry_at=next_retry_at,
                attempt_identities=attempt_identities,
            )
            attempt_operation.succeeded += 1
            attempt_operation.metadata.setdefault("outcomes", []).append(
                _scope_attempt_payload(state, plan_signature=scope_plan_signature)
            )
        except Exception as exc:
            attempt_operation.errors.append(
                f"scope attempt {item.identity}: {type(exc).__name__}: {exc}"
            )

    if args.season_limit:
        deferred = work[args.season_limit :]
        work = work[: args.season_limit]
        work_plan.skipped += len(deferred)
        work_plan.metadata["limit_deferred_scopes"] = [
            format_scope_token(item.competition_id, item.source_season_key)
            for item in deferred
        ]
        deferred_at = datetime.now(timezone.utc).replace(tzinfo=None)
        deferred_tokens = [
            format_scope_token(item.competition_id, item.source_season_key)
            for item in deferred
        ]
        add_automatic_deferral(
            kind="budget",
            target_type="scope",
            targets=deferred_tokens,
            reason="configured season budget deferred scope",
        )
        for deferred_item in deferred:
            record_automatic_attempt(
                deferred_item,
                outcome="deferred",
                reason="configured season budget deferred scope",
                next_retry_at=deferred_at + timedelta(minutes=1),
            )
    work_plan.counts["planned_scopes"] = len(work)
    operations.append(work_plan)
    if automatic_catalog:
        operations.append(attempt_operation)
    completed_scopes: list[str] = []
    replay_gap_entries: list[dict[str, Any]] = []

    # Complete one exact scope end-to-end before starting the next.  This is
    # the fairness boundary that prevents a season-first pass from consuming
    # the whole request budget and permanently starving child entities.
    for work_index, item in enumerate(work):
        scope_key = format_scope_token(item.competition_id, item.source_season_key)
        observed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        deadline_at = getattr(args, "deadline_at", None)
        deferred_reason = None
        if deadline_at is not None and observed_at >= deadline_at:
            deferred_reason = "run deadline deferred scope"
        elif automatic_catalog and service.ledger.remaining_requests < max_attempts:
            deferred_reason = "request budget deferred scope"
        if deferred_reason is not None:
            deferred = work[work_index:]
            work_plan.skipped += len(deferred)
            work_plan.metadata.setdefault("runtime_deferred_scopes", []).extend(
                format_scope_token(value.competition_id, value.source_season_key)
                for value in deferred
            )
            add_automatic_deferral(
                kind=("deadline" if "deadline" in deferred_reason else "budget"),
                target_type="scope",
                targets=[
                    format_scope_token(value.competition_id, value.source_season_key)
                    for value in deferred
                ],
                reason=deferred_reason,
            )
            for deferred_item in deferred:
                record_automatic_attempt(
                    deferred_item,
                    outcome="deferred",
                    reason=deferred_reason,
                    next_retry_at=observed_at + timedelta(minutes=1),
                )
            break
        scope_operations = []
        operation, bundle = service.sync_season(
            item.competition_id,
            item.source_season_key,
            prefetched=selected_fetches.get(item.identity),
        )
        operations.append(operation)
        scope_operations.append(operation)
        if bundle is None:
            work_plan.retryable.append(f"scope {scope_key} has no season bundle")
        else:
            if "leaderboards" in entities:
                leaderboard_operation = service.sync_leaderboards(bundle)
                operations.append(leaderboard_operation)
                scope_operations.append(leaderboard_operation)

            if "matches" in entities:
                capacity = service.ledger.remaining_requests // max_attempts
                per_run_limit = args.match_limit or len(bundle.matches)
                match_operation = service.sync_match_payloads(
                    bundle,
                    limit=min(per_run_limit, capacity),
                )
                operations.append(match_operation)
                scope_operations.append(match_operation)

            player_ids: set[int] = set()
            if "teams" in entities:
                capacity = service.ledger.remaining_requests // max_attempts
                per_run_limit = args.team_limit or len(bundle.teams)
                team_operation, player_ids = service.sync_team_snapshots(
                    bundle,
                    limit=min(per_run_limit, capacity),
                    # An advertised team without a global /teams payload is a
                    # fact about the source in ANY season: besides removed
                    # deep-history clubs, current small-cup entrants and
                    # bracket placeholders (e.g. team id -1) never get a page
                    # (#1070: 96 proven null-body teams in latest seasons).
                    # The absence path commits EXCLUDED without tombstoning a
                    # last-good snapshot, so latest seasons are safe too.
                    allow_advertised_absence=True,
                )
                operations.append(team_operation)
                scope_operations.append(team_operation)

            if "players" in entities:
                if mode == RunMode.REPLAY:
                    # Raw replay performs no HTTP attempts and resolves each
                    # rotating Next.js target from its durable manifest.  A
                    # network request budget must not truncate a large squad.
                    capacity = len(player_ids)
                    build_reserve = 0
                else:
                    capacity = service.ledger.remaining_requests // max_attempts
                    build_reserve = 0 if args.next_build_id else max_attempts
                per_run_limit = args.player_limit or len(player_ids)
                player_operation = service.sync_player_snapshots(
                    player_ids,
                    build_id=(args.next_build_id or None),
                    limit=min(
                        per_run_limit,
                        max(0, capacity - build_reserve),
                    ),
                )
                operations.append(player_operation)
                scope_operations.append(player_operation)

        outstanding = {
            item.entity: _outstanding_targets(item)
            for item in scope_operations
            if _outstanding_targets(item)
        }
        source_missing_matches = sum(
            int(operation.metadata.get("intentional_not_available") or 0)
            for operation in scope_operations
            if operation.entity == "match_payloads"
        )
        scope_ok = (
            bool(bundle is not None)
            and all(item.ok for item in scope_operations)
            and not outstanding
            and source_missing_matches == 0
        )
        if scope_ok and bundle is not None:
            descriptors = (
                (
                    *bundle.player_categories,
                    *bundle.team_categories,
                )
                if "leaderboards" in entities
                else ()
            )
            expected_matches = (
                [
                    match.get("match_id")
                    for match in bundle.matches
                    if match.get("finished")
                ]
                if "matches" in entities
                else []
            )
            expected_teams = (
                [team.get("team_id") for team in bundle.teams]
                if "teams" in entities
                else []
            )
            coverage = {
                "scope_entities": sorted(scope_entities),
                "leaderboard_identity_hash": _identity_hash(
                    (
                        descriptor.participant_type,
                        descriptor.name,
                        descriptor.fetch_all_url,
                        descriptor.source_order,
                    )
                    for descriptor in descriptors
                ),
                "match_identity_hash": _identity_hash(expected_matches),
                "team_identity_hash": _identity_hash(expected_teams),
                "player_identity_hash": _identity_hash(
                    player_ids if "players" in entities else ()
                ),
            }
            counts = {
                "leaderboards": len(descriptors),
                "matches": len(expected_matches),
                "teams": len(expected_teams),
                "players": len(player_ids) if "players" in entities else 0,
            }
            completion = OperationResult(
                "scope_completion",
                attempted=1,
                metadata={"scope": scope_key, **coverage},
            )
            try:
                completion.tables.extend(
                    service.record_scope_completion(
                        item.competition_id,
                        item.source_season_key,
                        plan_signature=journal_plan_signature,
                        coverage=coverage,
                        counts=counts,
                    )
                )
                completion.succeeded = 1
                completion.counts["scopes"] = 1
                work_plan.succeeded += 1
                completed_scopes.append(scope_key)
                record_automatic_attempt(
                    item,
                    outcome="success",
                    reason="scope completion committed",
                    next_retry_at=(
                        observed_at
                        + _schedule_cooldown(bundle.matches, observed_at)
                        if automatic_lane == ScopeLane.CURRENT
                        else None
                    ),
                    attempt_identities=(f"{run_id}:{scope_key}",),
                )
            except Exception as exc:
                completion.errors.append(
                    f"scope {scope_key}: {type(exc).__name__}: {exc}"
                )
                record_automatic_attempt(
                    item,
                    outcome="terminal",
                    reason=f"scope completion commit failed: {type(exc).__name__}",
                    attempt_identities=(f"{run_id}:{scope_key}",),
                )
            operations.append(completion)
        else:
            previous_attempt = attempt_states.get(item.identity)
            attempt_identity = f"{run_id}:{scope_key}"
            prior_identities = (
                previous_attempt.attempt_identities
                if previous_attempt is not None
                else ()
            )
            source_gap = (
                automatic_catalog
                and source_missing_matches > 0
                and bool(bundle is not None)
                and all(operation.ok for operation in scope_operations)
                and not outstanding
                and previous_attempt is not None
                and previous_attempt.outcome in {"retryable", "source_gap"}
                and previous_attempt.reason
                in {_SOURCE_GAP_RETRY_REASON, _SOURCE_GAP_REASON}
                and len(set((*prior_identities, attempt_identity))) >= 2
            )
            budget_failure = any(
                _is_budget_deferral_error(error)
                for operation in scope_operations
                for error in operation.errors
            )
            hard_failure = any(operation.terminal for operation in scope_operations) or any(
                not _is_budget_deferral_error(error)
                for operation in scope_operations
                for error in operation.errors
            )
            work_plan_retryable = (
                f"scope {scope_key} incomplete; outstanding={outstanding}"
            )
            if source_gap:
                gap_identities = tuple(
                    dict.fromkeys((*prior_identities, attempt_identity))
                )[-2:]
                record_automatic_attempt(
                    item,
                    outcome="source_gap",
                    reason=_SOURCE_GAP_REASON,
                    next_retry_at=(
                        observed_at + timedelta(hours=48)
                        if automatic_lane == ScopeLane.CURRENT
                        else None
                    ),
                    attempt_identities=gap_identities,
                )
                work_plan.succeeded += 1
            else:
                if not hard_failure or not automatic_catalog:
                    work_plan.retryable.append(work_plan_retryable)
                next_due = _scope_retry_due(
                    (previous_attempt.attempt_count + 1 if previous_attempt else 1),
                    observed_at,
                )
                record_automatic_attempt(
                    item,
                    outcome="terminal" if hard_failure else "retryable",
                    reason=(
                        "schema or entity processing failure"
                        if hard_failure
                        else (
                            _SOURCE_GAP_RETRY_REASON
                            if source_missing_matches and not outstanding
                            else work_plan_retryable
                        )
                    ),
                    next_retry_at=None if hard_failure else next_due,
                    attempt_identities=tuple(
                        dict.fromkeys((*prior_identities, attempt_identity))
                    ),
                )
            work_plan.metadata.setdefault("incomplete_scopes", []).append(
                {"scope": scope_key, "outstanding": outstanding}
            )
            player_operations = [
                operation
                for operation in scope_operations
                if operation.entity == "player_snapshots"
            ]
            missing_player_ids = (
                _player_replay_missing_raw_ids(player_operations[0])
                if mode == RunMode.REPLAY and len(player_operations) == 1
                else None
            )
            other_operations = [
                operation
                for operation in scope_operations
                if not player_operations or operation is not player_operations[0]
            ]
            if (
                missing_player_ids is not None
                and all(operation.ok for operation in other_operations)
                and outstanding == {"player_snapshots": len(missing_player_ids)}
            ):
                replay_gap_entries.append(
                    {
                        "scope": scope_key,
                        "outstanding": outstanding,
                        "work_plan_retryable": work_plan_retryable,
                        "player_operation": player_operations[0],
                        "missing_player_ids": list(missing_player_ids),
                    }
                )

        if (
            not scope_ok
            and (
                service.ledger.remaining_requests < max_attempts
                or budget_failure
            )
            and work_index + 1 < len(work)
        ):
            deferred = work[work_index + 1 :]
            work_plan.skipped += len(deferred)
            work_plan.metadata["budget_deferred_scopes"] = [
                f"{deferred_item.competition_id}={deferred_item.source_season_key}"
                for deferred_item in deferred
            ]
            work_plan.retryable.append(
                f"request budget deferred {len(deferred)} remaining scopes"
            )
            deferred_at = datetime.now(timezone.utc).replace(tzinfo=None)
            for deferred_item in deferred:
                record_automatic_attempt(
                    deferred_item,
                    outcome="deferred",
                    reason="request budget deferred scope",
                    next_retry_at=deferred_at + timedelta(minutes=1),
                )
            add_automatic_deferral(
                kind="budget",
                target_type="scope",
                targets=[
                    format_scope_token(
                        deferred_item.competition_id,
                        deferred_item.source_season_key,
                    )
                    for deferred_item in deferred
                ],
                reason="request budget deferred scope",
            )
            break

    if "transfers" in entities:
        if automatic_contract is not None:
            # Журнал трансферов — та же история попыток: ключом идёт стабильная
            # подпись, а в отчёт (selection.transfer_plan_signature) уходит
            # контрактная, которую сверяет приёмка.
            transfer_signature = journal_plan_signature
        else:
            transfer_policy = {
                "window": transfer_window,
                "pagination": "unique_hits",
            }
            transfer_signature = deterministic_plan_signature(
                {"transfers"},
                policy=transfer_policy,
            )
        completed_transfer_ids = set()
        transfer_completion_times = {}
        if mode == RunMode.BACKFILL:
            completed_transfer_ids = service.repository.completed_competition_ids(
                transfer_signature,
            )
        elif mode == RunMode.DAILY:
            transfer_completion_times = service.repository.competition_completion_times(
                transfer_signature
            )
        competition_ids = [
            item.competition.competition_id
            for item in all_candidates
            if item.competition.competition_id not in completed_transfer_ids
        ]
        if mode == RunMode.DAILY:
            competition_ids.sort(
                key=lambda competition_id: (
                    transfer_completion_times.get(competition_id, datetime.min),
                    competition_id,
                )
            )
        profile_validation_deferred = [
            competition_id
            for competition_id in competition_ids
            if competition_id not in freshly_validated_included_ids
        ]
        competition_ids = [
            competition_id
            for competition_id in competition_ids
            if competition_id in freshly_validated_included_ids
        ]
        if args.competition_limit:
            deferred_by_limit = competition_ids[args.competition_limit :]
            competition_ids = competition_ids[: args.competition_limit]
        else:
            deferred_by_limit = []
        transfer_plan = OperationResult(
            "transfer_work_plan",
            attempted=len(competition_ids),
            metadata={
                "plan_signature": transfer_signature,
                "window": transfer_window,
                "already_complete_competitions": len(completed_transfer_ids),
                "daily_completion_timestamps": len(transfer_completion_times),
                "limit_deferred_competition_ids": deferred_by_limit,
                "profile_validation_deferred_competition_ids": (
                    profile_validation_deferred
                ),
            },
        )
        transfer_plan.skipped = len(deferred_by_limit) + len(
            profile_validation_deferred
        )
        operations.append(transfer_plan)
        add_automatic_deferral(
            kind="budget",
            target_type="transfer",
            targets=profile_validation_deferred,
            reason=(
                "competition discovery budget deferred fresh profile "
                "validation for transfer"
            ),
        )
        add_automatic_deferral(
            kind="budget",
            target_type="transfer",
            targets=deferred_by_limit,
            reason="configured competition budget deferred transfer",
        )
        completed_transfer_competition_ids: list[int] = sorted(
            completed_transfer_ids
        )
        for index, competition_id in enumerate(competition_ids):
            observed_at = datetime.now(timezone.utc).replace(tzinfo=None)
            deadline_at = getattr(args, "deadline_at", None)
            if deadline_at is not None and observed_at >= deadline_at:
                deferred = competition_ids[index:]
                transfer_plan.skipped += len(deferred)
                transfer_plan.metadata["deadline_deferred_competition_ids"] = deferred
                add_automatic_deferral(
                    kind="deadline",
                    target_type="transfer",
                    targets=deferred,
                    reason="run deadline deferred transfer",
                )
                break
            capacity = service.ledger.remaining_requests // max_attempts
            max_pages = min(args.transfer_max_pages, capacity)
            if not max_pages:
                deferred = competition_ids[index:]
                transfer_plan.skipped += len(deferred)
                transfer_plan.retryable.append(
                    f"request budget deferred {len(deferred)} transfer streams"
                )
                transfer_plan.metadata["budget_deferred_competition_ids"] = deferred
                add_automatic_deferral(
                    kind="budget",
                    target_type="transfer",
                    targets=deferred,
                    reason="request budget deferred transfer",
                )
                break
            transfer_operation = service.sync_transfers(
                competition_id,
                max_pages=max_pages,
                recent_only=(mode == RunMode.DAILY),
            )
            operations.append(transfer_operation)
            expected_hits = transfer_operation.metadata.get("source_hits")
            observed_events = int(transfer_operation.counts.get("events") or 0)
            tolerated_deficit = int(
                transfer_operation.metadata.get("source_hits_deficit") or 0
            )
            complete_stream = (
                transfer_operation.ok
                and expected_hits is not None
                and observed_events + tolerated_deficit >= int(expected_hits)
            )
            if complete_stream:
                completion = OperationResult(
                    "competition_completion",
                    attempted=1,
                    metadata={
                        "competition_id": competition_id,
                        "window": transfer_window,
                    },
                )
                try:
                    completion.tables.extend(
                        service.record_competition_completion(
                            competition_id,
                            plan_signature=transfer_signature,
                            coverage={
                                "window": transfer_window,
                                "source_hits": int(expected_hits),
                                "observed_events": observed_events,
                                "source_hits_deficit": tolerated_deficit,
                            },
                            counts={"events": observed_events},
                        )
                    )
                    completion.succeeded = 1
                    completion.counts["competitions"] = 1
                    transfer_plan.succeeded += 1
                    completed_transfer_competition_ids.append(competition_id)
                except Exception as exc:
                    completion.errors.append(
                        f"competition {competition_id}: {type(exc).__name__}: {exc}"
                    )
                operations.append(completion)
            else:
                transfer_plan.retryable.append(
                    f"competition {competition_id} transfer stream incomplete"
                )
        # Покрытие трансферов — свойство обязательства, а не одного окна.
        # Под стабильной подписью бэкфил перестал перезабирать уже закрытые
        # потоки, поэтому список этого рана перестал совпадать с included_ids,
        # а приёмка спрашивает именно про них: у дренированной полосы
        # (закрывать нечего, ран зелёный) это дало бы «transfer completion
        # evidence is incomplete». Добираем ранее закрытые, ограничив их
        # текущим контрактом.
        durable_transfer_ids = (
            set(completed_transfer_ids) & set(automatic_contract.included_ids)
            if automatic_contract is not None
            else set(completed_transfer_ids)
        )
        completed_transfer_competition_ids = sorted(
            set(completed_transfer_competition_ids) | durable_transfer_ids
        )
    else:
        completed_transfer_competition_ids = []

    # Доказательства рана — это исходы, которые записал ОН САМ. Раньше здесь
    # перечитывалась карта состояний: под контентной подписью она совпадала с
    # работой рана, под стабильной содержит историю всей полосы, и отчёт начал
    # бы утверждать чужое — отсрочки прошлых окон без оснований в этом ране,
    # состояния скоупов, которых в плане не было.
    run_attempt_outcomes = {
        (int(entry["competition_id"]), str(entry["source_season_key"])): entry
        for entry in attempt_operation.metadata.get("outcomes", ())
        if isinstance(entry, Mapping)
    }
    deferred_scope_targets = {
        target
        for evidence in automatic_deferrals
        if evidence.get("target_type") == "scope"
        for target in evidence.get("targets", [])
    }
    unauthorized_deferred_attempts = False
    # Только исходы ЭТОГО рана: под стабильной журнальной подписью карта
    # состояний хранит историю всей полосы, и отсрочки чужих ранов ушли бы в
    # selection.deferrals как доказательства этого — приёмка справедливо
    # ответила бы «deferral target вне контракта».
    for entry in run_attempt_outcomes.values():
        if entry.get("outcome") != "deferred":
            continue
        token = format_scope_token(
            int(entry["competition_id"]), str(entry["source_season_key"])
        )
        if token in deferred_scope_targets:
            continue
        state_reason = str(entry.get("reason") or "")
        reason = state_reason.casefold()
        if "deadline" in reason:
            kind = "deadline"
        elif "budget" in reason:
            kind = "budget"
        else:
            kind = None
        if kind is None:
            unauthorized_deferred_attempts = True
            continue
        add_automatic_deferral(
            kind=kind,
            target_type="scope",
            targets=[token],
            reason=state_reason,
        )
        deferred_scope_targets.add(token)
    rc, payload = finish()
    replay_missing_inputs = (
        _replay_missing_raw_evidence(
            operations=operations,
            work_plan=work_plan,
            planned_scopes=planned_scopes,
            completed_scopes=completed_scopes,
            gap_entries=replay_gap_entries,
        )
        if mode == RunMode.REPLAY
        else None
    )
    payload["selection"] = {
        "entities": sorted(entities),
        "explicit_scopes": [
            format_scope_token(competition_id, season)
            for competition_id, season in explicit_scopes
        ],
        "competition_limit": args.competition_limit,
        "season_limit": args.season_limit,
        "scope_plan_signature": scope_plan_signature,
        "planned_scopes": planned_scopes,
        "completed_scopes": completed_scopes,
        "completed_transfer_competition_ids": (completed_transfer_competition_ids),
        "requests_per_minute": args.requests_per_minute,
    }
    if replay_missing_inputs is not None:
        payload["selection"]["replay_missing_player_inputs"] = replay_missing_inputs
    daily_contract = getattr(args, "daily_competition_contract", None)
    if daily_contract:
        payload["selection"]["daily_contract"] = daily_contract["schema"]
        payload["selection"]["competition_scope"] = daily_contract
    if automatic_contract is not None:
        payload["selection"].update(
            {
                "entities": list(automatic_contract.entities),
                "scope_lane": automatic_lane.value,
                "catalog_contract": automatic_contract.as_dict(),
                "catalog_ids": catalog_ids_evidence,
                "catalog_decisions": catalog_decisions_evidence,
                "scope_attempts": [
                    entry for _identity, entry in sorted(run_attempt_outcomes.items())
                ],
                "transfer_plan_signature": (
                    scope_plan_signature if "transfers" in entities else None
                ),
                "deferrals": automatic_deferrals,
            }
        )
        # Гейт судит ран по тому, что сделал он сам. Карта состояний под
        # стабильной подписью — это история всей полосы, включая скоупы, до
        # которых это окно не дошло.
        run_outcomes = tuple(
            str(entry.get("outcome"))
            for entry in attempt_operation.metadata.get("outcomes", ())
            if isinstance(entry, Mapping)
        )
        retryable_attempts = "retryable" in run_outcomes
        terminal_attempts = "terminal" in run_outcomes
        # Закрытый скоуп — единственное доказательство, что ран вообще работал.
        # source_gap закрывает скоуп с признанной дырой источника, это тоже
        # продвижение обхода.
        scope_progress = any(
            outcome in {"success", "source_gap"} for outcome in run_outcomes
        )
        # Продвижение рана числом, а не только цветом: цвет отвечает на вопрос
        # «ран сломан?», а мониторингу нужен вопрос «сколько он закрыл из того,
        # что планировал». Один закрытый скоуп из двухсот — формально жёлтый
        # ран, но по этим числам он виден сразу.
        payload["selection"]["scope_outcome_counts"] = {
            outcome: run_outcomes.count(outcome)
            for outcome in sorted(set(run_outcomes))
        }
        payload["selection"]["planned_scope_count"] = len(planned_scopes)
        unauthorized_operation_retries = any(
            not _is_budget_deferral_error(reason)
            for operation in operations
            for reason in operation.retryable
        )
        hard_failures = any(
            operation.terminal
            or any(
                not _is_budget_deferral_error(error)
                for error in operation.errors
            )
            for operation in operations
        )
        # Непрерывная полоса по автоматическому каталогу физически не может
        # закрыть все ~450 скоупов за одно окно: скоуп, отложенный на повтор
        # (retryable), — это штатное промежуточное состояние обхода, а не отказ
        # рана. Красным ран делают только неустранимые исходы: terminal,
        # жёсткие ошибки операций и отсрочка без бюджетного/дедлайнового
        # основания (последнее — проверка честности доказательств).
        #
        # ВАЖНО: послабление действует только когда ран реально продвинулся.
        # Ран, не закрывший НИ ОДНОГО скоупа и собравший одни повторы (источник
        # лёг, прокси мёртв, сеть отрезана), обязан остаться красным — иначе
        # получается зелёный ран при нулевом сборе, ровно та слепота, из-за
        # которой простой контура дважды находили через сутки.
        no_progress_failure = (
            retryable_attempts or unauthorized_operation_retries
        ) and not scope_progress
        if (
            terminal_attempts
            or unauthorized_deferred_attempts
            or hard_failures
            or no_progress_failure
        ):
            payload["status"] = "incomplete"
            payload["complete"] = False
            rc = 1
        elif (
            retryable_attempts
            or unauthorized_operation_retries
            or automatic_deferrals
        ):
            payload["status"] = "partial_success"
            payload["complete"] = False
            rc = 0
    return rc, payload


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run source-native FotMob ingestion",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--mode",
        choices=NATIVE_MODES,
        required=True,
        help="Source-native discovery/ingestion mode",
    )
    parser.add_argument(
        "--publication-generation-id",
        default="",
        help="Exact ControlStore publication generation UUID",
    )
    parser.add_argument("--publication-schema", default="")
    parser.add_argument("--publication-source", default="")
    parser.add_argument("--publication-owner", default="")
    parser.add_argument("--publication-data-interval-start", default="")
    parser.add_argument("--publication-data-interval-end", default="")
    parser.add_argument("--publication-runtime-fingerprint", default="")
    parser.add_argument(
        "--run-id",
        default="",
        help="Must be absent or exactly equal the publication generation UUID",
    )
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        metavar="ID=SEASON",
        help="Exact native scope; repeat or comma-separate (e.g. 47=2025/2026)",
    )
    parser.add_argument(
        "--catalog-contract",
        default="",
        help="Dynamic automatic catalog contract schema",
    )
    parser.add_argument(
        "--daily-contract",
        default="",
        help="Exact admitted daily workload contract schema",
    )
    parser.add_argument(
        "--competition-scope-file",
        default="",
        help="Immutable #930 scope artifact used to derive daily competition IDs",
    )
    parser.add_argument("--competition-scope-sha256", default="")
    parser.add_argument("--competition-ids-sha256", default="")
    parser.add_argument(
        "--source-refresh-profile",
        default="",
        help="Reviewed bounded player source-refresh profile",
    )
    parser.add_argument(
        "--source-refresh-targets-sha256",
        default="",
        help="Exact SHA-256 of the reviewed source-refresh target artifact",
    )
    parser.add_argument(
        "--raw-store-uri",
        default="",
        help="Required in native mode; defaults to FOTMOB_RAW_STORE_URI",
    )
    parser.add_argument("--max-requests", type=int, default=2000)
    parser.add_argument("--max-direct-mib", type=float, default=256.0)
    parser.add_argument("--max-proxy-mib", type=float, default=0.0)
    parser.add_argument("--requests-per-minute", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--commit-batch-size",
        type=int,
        default=50,
        help=(
            "Targets buffered into one Iceberg commit per table. 1 commits "
            "every target separately (one single-row data file per target)."
        ),
    )
    parser.add_argument(
        "--max-buffered-rows",
        type=int,
        default=100_000,
        help=(
            "Physical rows buffered before an early flush. The 20k default "
            "of the repository flushed every ~4 matches once field-inventory "
            "rows piled up, defeating --commit-batch-size. Only effective "
            "with --commit-batch-size > 1 (batch size 1 writes unbuffered)."
        ),
    )
    parser.add_argument("--competition-limit", type=int, default=0)
    parser.add_argument("--season-limit", type=int, default=0)
    parser.add_argument("--match-limit", type=int, default=0)
    parser.add_argument("--team-limit", type=int, default=0)
    parser.add_argument("--player-limit", type=int, default=0)
    parser.add_argument(
        "--next-build-id",
        default="",
        help=(
            "Optional exact FotMob Next build id override; offline replay "
            "normally resolves each player's historical raw target from manifests"
        ),
    )
    parser.add_argument("--transfer-max-pages", type=int, default=250)
    parser.add_argument(
        "--deadline",
        default="",
        help="Cooperative UTC deadline (ISO-8601); remaining scopes are deferred",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Atomic report path; default is run-specific under /tmp",
    )
    parser.add_argument(
        "--entities",
        type=str,
        default="",
        help="Comma-separated source-native entity subset",
    )
    return parser


def _validate_args(
    parser: argparse.ArgumentParser, args
) -> dict[str, Any] | None:
    positive = {
        "--max-requests": args.max_requests,
        "--max-direct-mib": args.max_direct_mib,
        "--requests-per-minute": args.requests_per_minute,
        "--max-attempts": args.max_attempts,
        "--workers": args.workers,
        "--commit-batch-size": args.commit_batch_size,
        "--transfer-max-pages": args.transfer_max_pages,
        "--max-buffered-rows": args.max_buffered_rows,
    }
    for name, value in positive.items():
        if value <= 0:
            parser.error(f"{name} must be positive")
    for name, value in {
        "--competition-limit": args.competition_limit,
        "--season-limit": args.season_limit,
        "--match-limit": args.match_limit,
        "--team-limit": args.team_limit,
        "--player-limit": args.player_limit,
        "--max-proxy-mib": args.max_proxy_mib,
    }.items():
        if value < 0:
            parser.error(f"{name} must be non-negative")
    if args.max_proxy_mib != 0:
        parser.error("--max-proxy-mib must remain 0 for the direct-only pipeline")
    if args.next_build_id and not re.fullmatch(r"[A-Za-z0-9_-]+", args.next_build_id):
        parser.error("--next-build-id contains unsupported characters")
    if args.workers > 16:
        parser.error("--workers must be <= 16")
    args.deadline_at = None
    if args.deadline:
        try:
            deadline = datetime.fromisoformat(str(args.deadline).replace("Z", "+00:00"))
        except ValueError:
            parser.error("--deadline must be an ISO-8601 timestamp")
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            parser.error("--deadline must include a UTC offset")
        deadline = deadline.astimezone(timezone.utc).replace(tzinfo=None)
        args.deadline_at = deadline
    automatic_catalog = bool(str(args.catalog_contract or "").strip())
    if automatic_catalog and args.catalog_contract != "fotmob-catalog-v1":
        parser.error("--catalog-contract must be fotmob-catalog-v1")
    try:
        source_refresh = _load_source_refresh_contract(args)
    except ValueError as exc:
        parser.error(f"invalid FotMob source-refresh profile: {exc}")
    if source_refresh is not None:
        from scrapers.fotmob.source_refresh import (
            PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
            PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        )

        literal_entities = frozenset(
            item.strip().casefold()
            for item in str(args.entities or "").split(",")
            if item.strip()
        )
        violations = []
        if automatic_catalog:
            violations.append("catalog contract must be empty")
        if args.mode != "backfill":
            violations.append("mode must be backfill")
        if _parse_scopes(args.scope):
            violations.append("exact season scope must be empty")
        if literal_entities != frozenset({"players"}):
            violations.append("entities must be exactly players")
        if any(
            value != 0
            for value in (
                args.competition_limit,
                args.season_limit,
                args.match_limit,
                args.team_limit,
                args.player_limit,
            )
        ):
            violations.append("all planner limits must be zero")
        if args.next_build_id:
            violations.append("Next build override must be empty")
        if args.max_requests != PLAYER_SOURCE_REFRESH_MAX_REQUESTS:
            violations.append("request budget")
        if args.max_direct_mib != PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB:
            violations.append("direct-byte budget")
        if args.max_attempts != 4:
            violations.append("max attempts")
        if args.requests_per_minute != 30:
            violations.append("request rate")
        if violations:
            parser.error(
                "invalid FotMob source-refresh execution: " + ", ".join(violations)
            )
    args.source_refresh_contract = source_refresh
    daily_contract_fields = (
        args.daily_contract,
        args.competition_scope_file,
        args.competition_scope_sha256,
        args.competition_ids_sha256,
    )
    if args.mode == PLAYER_COLLECTOR_MODE:
        literal_entities = tuple(
            sorted(
                {
                    item.strip().casefold()
                    for item in str(args.entities or "").split(",")
                    if item.strip()
                }
            )
        )
        violations = []
        if automatic_catalog:
            violations.append("catalog contract must be empty")
        if source_refresh is not None:
            violations.append("source refresh must be empty")
        if any(daily_contract_fields):
            violations.append("daily contract fields must be empty")
        if _parse_scopes(args.scope):
            violations.append("exact season scope must be empty")
        if literal_entities != PLAYER_COLLECTOR_ENTITIES:
            violations.append("entities must be exactly players")
        if any(
            value != 0
            for value in (
                args.competition_limit,
                args.season_limit,
                args.match_limit,
                args.team_limit,
            )
        ):
            violations.append("non-player planner limits must be zero")
        if args.player_limit != PLAYER_COLLECTOR_PLAYER_LIMIT:
            violations.append("player limit")
        if args.max_requests != PLAYER_COLLECTOR_MAX_REQUESTS:
            violations.append("request budget")
        if args.max_direct_mib != PLAYER_COLLECTOR_MAX_DIRECT_MIB:
            violations.append("direct-byte budget")
        if args.requests_per_minute != PLAYER_COLLECTOR_REQUESTS_PER_MINUTE:
            violations.append("request rate")
        if args.max_attempts != 4:
            violations.append("max attempts")
        if args.workers != 4:
            violations.append("workers")
        if args.next_build_id:
            violations.append("Next build override must be empty")
        if args.deadline:
            violations.append("deadline must be empty")
        if violations:
            parser.error(
                "invalid FotMob player collector: " + ", ".join(violations)
            )
    if automatic_catalog:
        violations = []
        if any(daily_contract_fields):
            violations.append("legacy daily contract fields must be empty")
        if _parse_scopes(args.scope):
            violations.append("automatic exact season scope must be empty")
        if args.mode not in {"daily", "refresh", "backfill"}:
            violations.append("mode must be daily, refresh, or backfill")
        if args.competition_limit:
            violations.append("competition limit must be zero")
        if violations:
            parser.error(
                "invalid FotMob automatic catalog profile: " + ", ".join(violations)
            )
        args.daily_competition_contract = None
        args.daily_competition_ids = ()
    elif args.mode == "daily":
        from utils.fotmob_publication import (
            FOTMOB_DAILY_COMPETITION_IDS_SHA256,
            FOTMOB_DAILY_CONTRACT_SCHEMA,
            FOTMOB_DAILY_ENTITIES,
            FOTMOB_DAILY_MAX_DIRECT_MIB,
            FOTMOB_DAILY_MAX_REQUESTS,
            FOTMOB_DAILY_REQUESTS_PER_MINUTE,
            FOTMOB_DAILY_SCOPE_FILE,
            FOTMOB_DAILY_SCOPE_SHA256,
            load_fotmob_daily_competition_contract,
        )

        violations = []
        if args.daily_contract != FOTMOB_DAILY_CONTRACT_SCHEMA:
            violations.append("daily contract schema")
        if args.competition_scope_file != FOTMOB_DAILY_SCOPE_FILE:
            violations.append("daily scope path")
        if args.competition_scope_sha256 != FOTMOB_DAILY_SCOPE_SHA256:
            violations.append("daily scope SHA")
        if args.competition_ids_sha256 != FOTMOB_DAILY_COMPETITION_IDS_SHA256:
            violations.append("daily competition ID SHA")
        if _parse_scopes(args.scope):
            violations.append("daily exact season scope must be empty")
        if _parse_native_entities(args.entities) != frozenset(FOTMOB_DAILY_ENTITIES):
            violations.append("daily entities")
        if args.max_requests != FOTMOB_DAILY_MAX_REQUESTS:
            violations.append("daily request budget")
        if args.max_direct_mib != FOTMOB_DAILY_MAX_DIRECT_MIB:
            violations.append("daily direct-byte budget")
        if args.requests_per_minute != FOTMOB_DAILY_REQUESTS_PER_MINUTE:
            violations.append("daily request rate")
        if args.competition_limit != 0 or args.season_limit != 0:
            violations.append("daily planner limits")
        if violations:
            parser.error(
                "invalid FotMob production daily profile: " + ", ".join(violations)
            )
        try:
            contract = load_fotmob_daily_competition_contract(
                args.competition_scope_file,
                scope_sha256=args.competition_scope_sha256,
                competition_ids_sha256=args.competition_ids_sha256,
            )
        except ValueError as exc:
            parser.error(f"invalid FotMob production daily scope: {exc}")
        args.daily_competition_contract = contract
        args.daily_competition_ids = tuple(contract["competition_ids"])
    else:
        if any(daily_contract_fields):
            parser.error("daily competition contract is valid only in daily mode")
        args.daily_competition_contract = None
        args.daily_competition_ids = ()
    try:
        publication = _publication_from_args(args)
    except Exception as exc:
        parser.error(f"invalid FotMob publication: {exc}")
    if (
        publication is not None
        and args.run_id
        and args.run_id != publication["generation_id"]
    ):
        parser.error("--run-id must exactly equal --publication-generation-id")
    return publication


def _sigterm_to_exception(signum, frame):
    """The driver's unit timeout sends TERM (then KILL after 30s). Raising here
    routes shutdown through main()'s failure path: salvage flush + a real
    report instead of a silent NO_REPORT kill."""

    service = _ACTIVE_NATIVE_SERVICE
    if service is not None:
        cancel = getattr(service, "cancel", None)
        if cancel is not None:
            cancel()
    raise RuntimeError(f"terminated by signal {signum}")


def _failure_payload(args, exc: Exception) -> dict[str, Any]:
    return {
        "run_id": args.run_id,
        "mode": args.mode,
        "status": "incomplete",
        "complete": False,
        "tables": [],
        "rows": {},
        "errors": [f"{type(exc).__name__}: {exc}"],
    }


def _run_native_unfenced(args) -> tuple[int, dict[str, Any]]:
    """Run ingestion with salvage-flush failure handling and no DB guard.

    Ceremony-free runs (no publication identity) call this directly;
    guarded runs wrap it in ``_run_native_under_fence``.
    """

    try:
        return _run_native(args)
    except (ValueError, RuntimeError) as exc:
        logger.error("FotMob runner configuration/runtime failure: %s", exc)
        _salvage_flush()
        return 1, _failure_payload(args, exc)
    except Exception as exc:
        logger.exception("Unexpected FotMob runner failure")
        _salvage_flush()
        return 1, _failure_payload(args, exc)


def _run_native_under_fence(
    args,
    publication: Mapping[str, Any],
) -> tuple[int, dict[str, Any]]:
    """Run ingestion and every salvage write inside the active DB guard."""

    _attest_native_runtime(args, publication)
    with _native_writer_fence(publication):
        try:
            return _run_native_unfenced(args)
        finally:
            # Every normal and salvage write has finished, but the DB guard is
            # still held. Drift here turns the run red before publication can
            # leave the writing phase.
            _attest_native_runtime(args, publication)


def main():
    # ``main`` is normally one-shot, but tests and embedded invocations can
    # call it repeatedly in one interpreter. A late TERM must never cancel a
    # completed prior run.
    _deactivate_native_service()
    parser = _argument_parser()
    args = parser.parse_args()
    try:
        environment_scopes = _scope_groups_from_environment()
    except ValueError as exc:
        parser.error(f"invalid {FOTMOB_SCOPE_JSON_ENV}: {exc}")
    if args.scope and environment_scopes:
        parser.error("--scope and FOTMOB_SCOPE_JSON cannot both be supplied")
    if environment_scopes:
        args.scope = environment_scopes
    publication = _validate_args(parser, args)
    if publication is not None:
        args.publication_generation_id = publication["generation_id"]
        args.run_id = publication["generation_id"]
    else:
        # A ceremony-disabled stub still names the run — keep it as the run id
        # so retries stay idempotent — but it is NOT a writer generation: no
        # fence is acquired for it, so ``_run_native`` must not go looking for
        # an active guard that by design never exists.
        args.run_id = args.run_id or (
            str(args.publication_generation_id or "").strip() or None
        )
        args.publication_generation_id = None
    output = args.output or f"/tmp/fotmob_result_{_safe_run_id(args.run_id)}.json"
    try:
        signal.signal(signal.SIGTERM, _sigterm_to_exception)
    except ValueError:
        pass  # not in the main thread (unit-test harness) — keep default
    try:
        # Замок писателя охватывает ОБА входа: ceremony-free ран не берёт
        # ControlStore-guard вовсе, и без этого захвата ручной добор пишет в
        # bronze параллельно полосе (B7).
        with _writer_lock():
            if publication is None:
                # Ceremony-free run: no ControlStore guard and no attestation,
                # exactly the pre-#995 runner semantics.
                rc, payload = _run_native_unfenced(args)
            else:
                rc, payload = _run_native_under_fence(args, publication)
    except Exception as exc:
        # Guard acquisition/validation failed before the service was built.
        # Do not salvage-flush here: there is no publication authority.
        logger.error("FotMob native writer fence failed: %s", exc)
        payload = _failure_payload(args, exc)
        rc = 1
    _deactivate_native_service()
    _write_json_atomic(output, payload)
    logger.info("FotMob report: %s", output)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return rc


if __name__ == "__main__":
    sys.exit(main())
