"""Runtime-safe grouping and persistence for signed SofaScore workload plans."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Sequence

from scrapers.sofascore.workload_plan import (
    MATCH_BATCH_SIZE,
    PLAYER_BATCH_SIZE,
    AllocationRequest,
    SeasonWorkload,
    SignedDagRunPlan,
    WorkloadAllocation,
    WorkloadBudgetPolicy,
    WorkloadPlanError,
    build_signed_allocation_plan,
    match_workload_class,
    parse_qualified_work_unit,
    player_workload_class,
    production_match_shape,
    production_player_shape,
    qualify_work_unit,
    source_tournament_token,
    stable_partitions,
    workload_shape_digest,
)


DEFAULT_PLAN_DIR = Path("/tmp/sofascore-workload-plans")


def partition_key(league: str, canonical_season: str) -> str:
    league = str(league).strip()
    canonical_season = str(canonical_season).strip()
    if not league or not canonical_season:
        raise WorkloadPlanError("workload partition needs league and season")
    return json.dumps(
        {"league": league, "season": canonical_season},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def parse_partition_key(value: str) -> tuple[str, str]:
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise WorkloadPlanError("workload partition key is corrupt") from exc
    if not isinstance(payload, Mapping) or set(payload) != {"league", "season"}:
        raise WorkloadPlanError("workload partition key has invalid fields")
    league = payload.get("league")
    season = payload.get("season")
    if not isinstance(league, str) or not league.strip():
        raise WorkloadPlanError("workload partition league is empty")
    if not isinstance(season, str) or not season.strip():
        raise WorkloadPlanError("workload partition season is empty")
    return league.strip(), season.strip()


@dataclass(frozen=True)
class PartitionWorkload:
    league: str
    canonical_season: str
    source_tournament_id: int | str
    pending_match_ids: tuple[str, ...] = ()
    player_universe_ids: tuple[str, ...] = ()
    pending_player_ids: tuple[str, ...] = ()
    season_workload: Optional[SeasonWorkload] = None

    @property
    def key(self) -> str:
        return partition_key(self.league, self.canonical_season)


def _group_slug(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def build_partitioned_plan(
    policy: WorkloadBudgetPolicy,
    *,
    dag_id: str,
    run_id: str,
    freshness_keys: Optional[Mapping[str, object]] = None,
    partitions: Sequence[PartitionWorkload],
    control_token: Optional[str | bytes] = None,
) -> SignedDagRunPlan:
    """Batch each partition independently and sign one whole logical run."""

    requests: list[AllocationRequest] = []
    full_universe: list[str] = []
    seen_keys: set[str] = set()
    match_shape_digest = workload_shape_digest(production_match_shape())
    player_shape_digest = workload_shape_digest(production_player_shape())
    for workload in sorted(partitions, key=lambda item: item.key):
        key = workload.key
        tournament_id = source_tournament_token(workload.source_tournament_id)
        if key in seen_keys:
            raise WorkloadPlanError(f"duplicate workload partition {key}")
        seen_keys.add(key)
        slug = _group_slug(key)
        universe_batches = stable_partitions(
            workload.player_universe_ids,
            max(1, len(workload.player_universe_ids)),
            field=f"{key}.player_universe_ids",
        )
        universe = universe_batches[0] if universe_batches else ()
        pending_players = stable_partitions(
            workload.pending_player_ids,
            max(1, len(workload.pending_player_ids)),
            field=f"{key}.pending_player_ids",
        )
        pending = pending_players[0] if pending_players else ()
        unknown = sorted(set(pending) - set(universe))
        if unknown:
            raise WorkloadPlanError(
                f"partition {key} pending players are outside full universe: {unknown[:5]}"
            )
        full_universe.extend(qualify_work_unit(key, value) for value in universe)
        for index, ids in enumerate(
            stable_partitions(
                workload.pending_match_ids,
                MATCH_BATCH_SIZE,
                field=f"{key}.pending_match_ids",
            )
        ):
            requests.append(
                AllocationRequest(
                    task_id=f"capture_match_{slug}_batch_{index:05d}",
                    scope="match",
                    workload_class=match_workload_class(),
                    batch_index=index,
                    units=tuple(qualify_work_unit(key, value) for value in ids),
                    source_tournament_id=tournament_id,
                    shape_digest=match_shape_digest,
                )
            )
        for index, ids in enumerate(
            stable_partitions(
                pending,
                PLAYER_BATCH_SIZE,
                field=f"{key}.pending_player_ids",
            )
        ):
            requests.append(
                AllocationRequest(
                    task_id=f"capture_player_{slug}_batch_{index:05d}",
                    scope="player",
                    workload_class=player_workload_class(),
                    batch_index=index,
                    units=tuple(qualify_work_unit(key, value) for value in ids),
                    source_tournament_id=tournament_id,
                    shape_digest=player_shape_digest,
                )
            )
        season = workload.season_workload
        if season is not None and season.pending:
            requests.append(
                AllocationRequest(
                    task_id=f"capture_season_{slug}_batch_00000",
                    scope="season",
                    workload_class=season.workload_class,
                    batch_index=0,
                    units=(qualify_work_unit(key, season.unit),),
                    source_tournament_id=tournament_id,
                    shape_digest=season.shape_digest,
                )
            )
    return build_signed_allocation_plan(
        policy,
        dag_id=dag_id,
        run_id=run_id,
        freshness_keys=freshness_keys,
        player_universe_ids=full_universe,
        requests=requests,
        control_token=control_token,
    )


def allocations_for_partition(
    plan: SignedDagRunPlan,
    *,
    league: str,
    canonical_season: str,
    scope: str,
) -> tuple[WorkloadAllocation, ...]:
    key = partition_key(league, canonical_season)
    selected: list[WorkloadAllocation] = []
    for allocation in plan.allocations:
        partitions = {parse_qualified_work_unit(unit)[0] for unit in allocation.units}
        if len(partitions) != 1:
            raise WorkloadPlanError(
                f"allocation {allocation.allocation_id} crosses partitions"
            )
        if key in partitions and allocation.scope == scope:
            selected.append(allocation)
    return tuple(sorted(selected, key=lambda item: item.batch_index))


def target_ids(allocation: WorkloadAllocation) -> tuple[str, ...]:
    return tuple(parse_qualified_work_unit(unit)[1] for unit in allocation.units)


def plan_path_for_run(
    dag_id: str,
    run_id: str,
    *,
    directory: Optional[os.PathLike[str] | str] = None,
) -> Path:
    root = Path(
        directory
        or os.environ.get("SOFASCORE_WORKLOAD_PLAN_DIR", "")
        or DEFAULT_PLAN_DIR
    )
    digest = hashlib.sha256(f"{dag_id}\0{run_id}".encode("utf-8")).hexdigest()
    return root / f"{digest}.json"


def write_plan(path: os.PathLike[str] | str, plan: SignedDagRunPlan) -> Path:
    """Create an immutable mode-0600 plan, accepting only identical retries."""

    destination = Path(path)
    encoded = (
        json.dumps(plan.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        existing = destination.read_bytes()
        if existing != encoded:
            raise WorkloadPlanError(
                f"DagRun plan already exists with different bytes: {destination}"
            )
        return destination
    temporary = destination.with_name(
        f"{destination.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != encoded:
                raise WorkloadPlanError(
                    f"concurrent DagRun plan differs: {destination}"
                )
        directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return destination


def load_plan(
    path: os.PathLike[str] | str,
    *,
    control_token: Optional[str | bytes] = None,
) -> SignedDagRunPlan:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkloadPlanError(f"cannot read signed workload plan: {path}") from exc
    return SignedDagRunPlan.from_dict(payload, control_token=control_token)
