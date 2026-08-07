"""Pure FotMob competition and season discovery helpers.

No function in this module performs I/O.  The caller owns fetching and raw
storage; this layer turns the returned JSON into auditable source identities.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import re
import unicodedata
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, Tuple

from .domain import (
    CompetitionRef,
    ParseIssue,
    ScopeClassification,
    ScopeDecision,
    SeasonRef,
)


class CatalogShapeError(ValueError):
    """Raised when a catalog/season payload cannot identify its source data."""


class CatalogConflictError(CatalogShapeError):
    """Raised in strict mode when one FotMob id has conflicting metadata."""


class SelectedSeasonMismatch(CatalogShapeError):
    """Raised when FotMob silently selects a season other than the requested one."""


@dataclass(frozen=True, slots=True)
class CatalogConflict:
    competition_id: int
    fields: Tuple[str, ...]
    variants: Tuple[CompetitionRef, ...]


@dataclass(frozen=True, slots=True)
class CatalogDiscovery:
    competitions: Tuple[CompetitionRef, ...]
    conflicts: Tuple[CatalogConflict, ...] = ()
    issues: Tuple[ParseIssue, ...] = ()

    @property
    def by_id(self) -> dict[int, CompetitionRef]:
        return {item.competition_id: item for item in self.competitions}


ClassifierHook = Callable[[CompetitionRef], Optional[ScopeClassification]]


# Названия матчатся после снятия диакритики (``_fold``), поэтому шаблоны — ASCII.
#
# ⚠️ Эти регулярки — СТРАХОВКА, а не основной механизм. Замер #1139 (06.08.2026,
# реплей raw store по 491 турниру): из 41 женского турнира каталога текущая версия
# ловила 2, доработанная ловит 19, а поле источника ``details.gender`` — все 41.
# У 22 турниров (Liga F, NWSL, WSL, Toppserien, WE League, A-Liga, SWPL 1,
# Copa de la Reina…) в названии нет гендерного маркера ни в каком виде, и никакой
# текстовый шаблон их не отличит. Их решает ``DEFAULT_SCOPE_OVERRIDES``.
#
# ⚠️ НЕ расширять широкими токенами без границ слова: ``damer`` внутри слова ловит
# ``Copa Sudamericana`` (144 матча), ``elite`` ловит ``Eliteserien`` (высший дивизион
# Норвегии), ``ii``/``b`` ловят вторые дивизионы (``NB II``, ``Liga II``,
# ``First Division B``). Исключённый турнир перестаёт фетчиться, но его строки в
# ``*_current`` не обнуляются, а замерзают — ошибка не оставляет улик.
_FEMALE_RE = re.compile(
    r"(?:\bwomen'?s?\b|\bwoman\b|\bfemale\b|\bfeminin\w*\b|\bfemenin\w*\b|\bfemenil\b|"
    r"\bfemmin\w*\b|\bfrauen\b|\bdamer\b|\bdonne\b|\bladies\b|\bkvinn\w*\b|\bkvinde\w*\b|"
    r"\bvrouwen\b|\(w\)(?:\s|$))",
    re.IGNORECASE,
)
_YOUTH_RE = re.compile(
    r"(?:\bu\s*-?\s*(?:17|18|19|20|21|22|23)\b|\bunder\s*-?\s*(?:17|18|19|20|21|22|23)\b|"
    r"\byouth\b|\bacademy\b|\bjunior(?:s)?\b|\bjuvenil\b|\bprimavera\b|\brevelacao\b)",
    re.IGNORECASE,
)
_RESERVE_RE = re.compile(
    r"(?:\breserve(?:s)?\b|\bdevelopment league\b|\bsecond teams?\b|\bii teams?\b|"
    r"\bpremier league 2(?:\s+div(?:ision)?\s+\d+)?\b)",
    re.IGNORECASE,
)
_FRIENDLY_RE = re.compile(
    r"(?:\bfriendly|friendlies\b|\bcharity\b|\bexhibition\b|\btestimonial\b)",
    re.IGNORECASE,
)


def _fold(text: str) -> str:
    """Drop diacritics so ``Féminine`` and ``Revelação`` match the ASCII patterns."""

    normalised = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalised if not unicodedata.combining(char))


def _nonempty(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _integer_id(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _source_key(value: Any, *, field: str) -> str:
    """Preserve a source season label without applying a season formula."""

    if isinstance(value, str):
        if value:
            return value
        raise CatalogShapeError(f"{field} is an empty season string")
    if isinstance(value, int) and not isinstance(value, bool):
        # Some historic fixtures encode a single-year season as a JSON number.
        # Stringifying is lossless; deriving an end-year would not be.
        return str(value)
    raise CatalogShapeError(f"{field} must be a string or integer, got {type(value).__name__}")


def _looks_like_competition(node: Mapping[str, Any], path: str) -> bool:
    item_id = _integer_id(node.get("id", node.get("leagueId", node.get("leagueID"))))
    name = _nonempty(node.get("name", node.get("leagueName", node.get("shortName"))))
    if item_id is None or name is None:
        return False
    if "leagues" in node and not node.get("pageUrl") and node.get("type") != "league":
        return False
    page_url = str(node.get("pageUrl") or node.get("url") or "")
    path_hint = re.search(
        r"(?:^|\.)(?:leagues|allLeagues|popular|international|competitions|tournaments)(?:\[\]|\.|$)",
        path,
        re.IGNORECASE,
    )
    return bool(
        path_hint
        or "/leagues/" in page_url
        or str(node.get("type") or "").lower() in {"league", "competition", "tournament"}
    )


def _iter_catalog_candidates(
    value: Any,
    *,
    path: str = "$",
    country_code: Optional[str] = None,
    country_name: Optional[str] = None,
) -> Iterable[tuple[Mapping[str, Any], str, Optional[str], Optional[str]]]:
    if isinstance(value, Mapping):
        next_code = country_code
        next_name = country_name
        if isinstance(value.get("leagues"), list):
            next_code = _nonempty(
                value.get("ccode", value.get("countryCode", value.get("code")))
            ) or country_code
            next_name = _nonempty(value.get("country", value.get("countryName")))
            if next_name is None and "name" in value:
                next_name = _nonempty(value.get("name"))
        if _looks_like_competition(value, path):
            yield value, path, next_code, next_name
        for key, child in value.items():
            child_path = f"{path}.{key}"
            yield from _iter_catalog_candidates(
                child,
                path=child_path,
                country_code=next_code,
                country_name=next_name,
            )
    elif isinstance(value, list):
        for child in value:
            yield from _iter_catalog_candidates(
                child,
                path=f"{path}[]",
                country_code=country_code,
                country_name=country_name,
            )


def _competition_from_candidate(
    node: Mapping[str, Any],
    path: str,
    context_code: Optional[str],
    context_name: Optional[str],
) -> CompetitionRef:
    item_id = _integer_id(node.get("id", node.get("leagueId", node.get("leagueID"))))
    name = _nonempty(node.get("name", node.get("leagueName", node.get("shortName"))))
    if item_id is None or name is None:  # guarded by _looks_like_competition
        raise CatalogShapeError(f"invalid competition candidate at {path}")
    page_url = _nonempty(node.get("pageUrl", node.get("url")))
    source_slug = _nonempty(node.get("seopath", node.get("slug")))
    if source_slug is None and page_url and "/" in page_url:
        source_slug = page_url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    return CompetitionRef(
        competition_id=item_id,
        name=name,
        country_code=_nonempty(
            node.get("ccode", node.get("countryCode", node.get("country")))
        ) or context_code,
        country_name=_nonempty(node.get("countryName")) or context_name,
        gender=_nonempty(node.get("gender")),
        competition_type=_nonempty(node.get("type", node.get("competitionType"))),
        age_group=_nonempty(node.get("ageGroup", node.get("age_group"))),
        page_url=page_url,
        source_slug=source_slug,
        source_paths=(path,),
    )


def _merge_competitions(variants: Sequence[CompetitionRef]) -> tuple[CompetitionRef, Tuple[str, ...]]:
    fields = (
        "name",
        "country_code",
        "country_name",
        "gender",
        "competition_type",
        "age_group",
        "page_url",
        "source_slug",
    )
    # Prefer the most complete catalog occurrence.  Path is a deterministic
    # tie-breaker and never participates in identity.
    base = sorted(
        variants,
        key=lambda item: (
            -sum(getattr(item, field) is not None for field in fields),
            item.source_paths,
        ),
    )[0]
    merged: dict[str, Any] = {"competition_id": base.competition_id}
    conflicts: list[str] = []
    for field in fields:
        values = [getattr(item, field) for item in variants if getattr(item, field) is not None]
        unique = list(dict.fromkeys(values))
        if len(unique) > 1 and field not in {"page_url", "source_slug"}:
            conflicts.append(field)
        merged[field] = getattr(base, field) if getattr(base, field) is not None else (unique[0] if unique else None)
    merged["source_paths"] = tuple(
        sorted({path for item in variants for path in item.source_paths})
    )
    return CompetitionRef(**merged), tuple(conflicts)


def discover_competitions(
    payload: Mapping[str, Any] | Sequence[Any],
    *,
    strict_conflicts: bool = False,
) -> CatalogDiscovery:
    """Discover and deduplicate all competition occurrences by numeric id.

    Duplicate entries in ``popular`` and country lists are expected.  They are
    merged, while contradictory names/countries/gender/type are retained in a
    structured conflict instead of being silently overwritten.
    """

    grouped: dict[int, list[CompetitionRef]] = {}
    issues: list[ParseIssue] = []
    for node, path, country_code, country_name in _iter_catalog_candidates(payload):
        try:
            item = _competition_from_candidate(node, path, country_code, country_name)
        except CatalogShapeError as exc:
            issues.append(ParseIssue("invalid_catalog_entry", path, str(exc)))
            continue
        grouped.setdefault(item.competition_id, []).append(item)

    if not grouped:
        raise CatalogShapeError("allLeagues payload contains no competition entries")

    competitions: list[CompetitionRef] = []
    conflicts: list[CatalogConflict] = []
    for competition_id in sorted(grouped):
        variants = grouped[competition_id]
        merged, conflict_fields = _merge_competitions(variants)
        competitions.append(merged)
        if conflict_fields:
            conflicts.append(
                CatalogConflict(competition_id, conflict_fields, tuple(variants))
            )

    if strict_conflicts and conflicts:
        rendered = ", ".join(
            f"{item.competition_id}({','.join(item.fields)})" for item in conflicts
        )
        raise CatalogConflictError(f"conflicting allLeagues entries: {rendered}")
    return CatalogDiscovery(tuple(competitions), tuple(conflicts), tuple(issues))


@dataclass(frozen=True, slots=True)
class ScopeOverride:
    """A reviewed per-id scope decision that outranks the name heuristics.

    Keyed by the immutable FotMob id, never by name: ``allLeagues`` serves three
    pairs of competitions whose name and country are byte-identical while the
    source itself reports different genders (237/11029 ``Super Cup`` [NED],
    10717/10557 and 10718/10558 ``UEFA Nations League A/B Qualification``).
    """

    competition_id: int
    decision: ScopeDecision
    name: str
    country: str
    reason: str = "women/female competition (source details.gender)"
    policy_rule: str = "exclude_female_source_gender"

    def __post_init__(self) -> None:
        if isinstance(self.competition_id, bool) or int(self.competition_id) <= 0:
            raise CatalogShapeError("scope override id must be a positive integer")
        if not str(self.name).strip():
            raise CatalogShapeError(
                f"scope override {self.competition_id} has no name for review"
            )


# Источник истины — поле ``details.gender`` из карточки лиги ``/leagues``.
# Каталог ``allLeagues`` его НЕ отдаёт (32 наблюдённых json-пути, пола среди них нет),
# поэтому решение по каталогу принимается по названию и течёт.
#
# Список снят 06.08.2026 реплеем raw store по 491 турниру, у которых карточка уже
# скачана: 450 ``male``, 41 ``female``, пустых нет. Все 41 были ``included``.
# Разбор: /root/E1139-E0-REPORT-2026-08-06.md, issue #1139.
#
# Пересматривать вместе с реестром: новый женский турнир попадёт сюда только после
# первой карточки, до этого его держит ``_FEMALE_RE`` (ловит примерно половину).
DEFAULT_SCOPE_OVERRIDES: Tuple[ScopeOverride, ...] = (
    ScopeOverride(256, ScopeDecision.EXCLUDED, "A-Liga", "Denmark"),
    ScopeOverride(331, ScopeDecision.EXCLUDED, "Toppserien", "Norway"),
    ScopeOverride(332, ScopeDecision.EXCLUDED, "1. Division Kvinner", "Norway"),
    ScopeOverride(333, ScopeDecision.EXCLUDED, "Norgesmesterskapet Kvinner", "Norway"),
    ScopeOverride(9089, ScopeDecision.EXCLUDED, "Damallsvenskan (W)", "Sweden"),
    ScopeOverride(9134, ScopeDecision.EXCLUDED, "NWSL", "United States"),
    ScopeOverride(9227, ScopeDecision.EXCLUDED, "WSL", "England"),
    ScopeOverride(9294, ScopeDecision.EXCLUDED, "WSL 2", "England"),
    ScopeOverride(9382, ScopeDecision.EXCLUDED, "Toppserien Qualification (W)", "Norway"),
    ScopeOverride(9500, ScopeDecision.EXCLUDED, "WE League", "Japan"),
    ScopeOverride(9677, ScopeDecision.EXCLUDED, "Première Ligue Féminine", "France"),
    ScopeOverride(9906, ScopeDecision.EXCLUDED, "Liga MX Femenil", "Mexico"),
    ScopeOverride(9907, ScopeDecision.EXCLUDED, "Liga F", "Spain"),
    ScopeOverride(9921, ScopeDecision.EXCLUDED, "SheBelieves Cup (W)", "International"),
    ScopeOverride(10167, ScopeDecision.EXCLUDED, "NWSL Challenge Cup", "United States"),
    ScopeOverride(10178, ScopeDecision.EXCLUDED, "Serie A Femminile", "Italy"),
    ScopeOverride(10269, ScopeDecision.EXCLUDED, "Womens Asian Cup", "International"),
    ScopeOverride(10289, ScopeDecision.EXCLUDED, "Eredivisie Vrouwen", "Netherlands"),
    ScopeOverride(10308, ScopeDecision.EXCLUDED, "Eliteettan (W)", "Sweden"),
    ScopeOverride(10316, ScopeDecision.EXCLUDED, "Damallsvenskan Qualification (W)", "Sweden"),
    ScopeOverride(10357, ScopeDecision.EXCLUDED, "World Cup Qualification (W) UEFA", "International"),
    ScopeOverride(
        10359,
        ScopeDecision.EXCLUDED,
        "World Cup Qualification (W) Inter-Confederation",
        "International",
    ),
    ScopeOverride(10434, ScopeDecision.EXCLUDED, "Serie A Femminile Qualification", "Italy"),
    ScopeOverride(10449, ScopeDecision.EXCLUDED, "Nacional Feminino", "Portugal"),
    ScopeOverride(
        10498,
        ScopeDecision.EXCLUDED,
        "Summer Olympics Qualification CONCACAF (W)",
        "International",
    ),
    # ⚠️ 10557 и 10558 ЗДЕСЬ СОЗНАТЕЛЬНО ОТСУТСТВУЮТ, хотя источник говорит female.
    # Карточка называет их "UEFA Women's Nations League A/B Qualification", каталог
    # срезает "Women's", и их приняли за мужские двойники 10717/10718. Оба вшиты в
    # опечатанную SHA-256 дневную когорту #930 (dags/utils/fotmob_publication.py),
    # причём пин продублирован в трёх независимых копиях. Их исключение уронит
    # validate_data и остановит дневную волну, поэтому вскрытие печати вынесено в
    # отдельную работу. Цена отсрочки — 26 матчей. См. #1139.
    ScopeOverride(10649, ScopeDecision.EXCLUDED, "NWSL x Liga MX", "International"),
    ScopeOverride(10651, ScopeDecision.EXCLUDED, "Copa de la Reina", "Spain"),
    ScopeOverride(10657, ScopeDecision.EXCLUDED, "Nacional Feminino Qualification", "Portugal"),
    ScopeOverride(10714, ScopeDecision.EXCLUDED, "1. divisjon kvinner kvalifisering", "Norway"),
    ScopeOverride(10791, ScopeDecision.EXCLUDED, "SWPL 1", "Scotland"),
    ScopeOverride(10872, ScopeDecision.EXCLUDED, "Northern Super League", "Canada"),
    ScopeOverride(11013, ScopeDecision.EXCLUDED, "CONCACAF W Champions Cup", "International"),
    ScopeOverride(11015, ScopeDecision.EXCLUDED, "Supercoppa Italiana", "Italy"),
    ScopeOverride(11019, ScopeDecision.EXCLUDED, "SWPL Cup", "Scotland"),
    ScopeOverride(11020, ScopeDecision.EXCLUDED, "SWF Scottish Cup", "Scotland"),
    ScopeOverride(11028, ScopeDecision.EXCLUDED, "Coupe de France Féminine", "France"),
    # 11029 — женский Super Cup; мужской с тем же именем и страной — 237.
    ScopeOverride(11029, ScopeDecision.EXCLUDED, "Super Cup", "Netherlands"),
    ScopeOverride(11315, ScopeDecision.EXCLUDED, "Concacaf W Qualifiers", "International"),
    ScopeOverride(11839, ScopeDecision.EXCLUDED, "WSL Promotion/Relegation", "England"),
    # Решение владельца от 07.08.2026: товарищеские матчи ВЗРОСЛЫХ МУЖСКИХ СБОРНЫХ
    # берём в сбор. Правило exclude_friendly для них перестаёт действовать, но
    # продолжает резать остальные три: 489 Club Friendlies (клубные, не сборные),
    # 10312 Sidemen Charity Match и 10656 Beta Squad vs Amp Charity (шоу-матчи).
    # 293 Women's Friendlies и 344 Friendlies U-21 режутся своими правилами.
    ScopeOverride(
        114,
        ScopeDecision.INCLUDED,
        "Friendlies",
        "International",
        reason="senior national-team friendlies are in scope by owner decision",
        policy_rule="include_national_team_friendlies",
    ),
)


@lru_cache(maxsize=4)
def _override_index(
    overrides: Tuple[ScopeOverride, ...]
) -> Mapping[int, ScopeOverride]:
    return {item.competition_id: item for item in overrides}


def classify_competition(
    competition: CompetitionRef,
    *,
    hooks: Sequence[ClassifierHook] = (),
    overrides: Sequence[ScopeOverride] = DEFAULT_SCOPE_OVERRIDES,
) -> ScopeClassification:
    """Apply the adult-men official-competition policy with override hooks."""

    for hook in hooks:
        decision = hook(competition)
        if decision is not None:
            if decision.competition.competition_id != competition.competition_id:
                raise ValueError("classifier hook returned a decision for another competition")
            return decision

    override = _override_index(tuple(overrides)).get(competition.competition_id)
    if override is not None:
        return ScopeClassification(
            competition, override.decision, override.reason, override.policy_rule
        )

    text = _fold(
        " ".join(
            part
            for part in (
                competition.name,
                competition.competition_type,
                competition.age_group,
            )
            if part
        )
    )
    gender = (competition.gender or "").strip().lower()
    if gender in {"female", "women", "woman", "f"} or _FEMALE_RE.search(text):
        return ScopeClassification(
            competition, ScopeDecision.EXCLUDED, "women/female competition", "exclude_female"
        )
    if _YOUTH_RE.search(text):
        return ScopeClassification(
            competition, ScopeDecision.EXCLUDED, "youth competition", "exclude_youth"
        )
    if _RESERVE_RE.search(text):
        return ScopeClassification(
            competition, ScopeDecision.EXCLUDED, "reserve/development competition", "exclude_reserve"
        )
    if _FRIENDLY_RE.search(text):
        return ScopeClassification(
            competition, ScopeDecision.EXCLUDED, "friendly/charity/exhibition", "exclude_friendly"
        )
    if gender and gender not in {"male", "men", "m"}:
        return ScopeClassification(
            competition,
            ScopeDecision.REVIEW_REQUIRED,
            f"unrecognized explicit gender {competition.gender!r}",
            "review_unknown_gender",
        )
    age_group = (competition.age_group or "").strip().lower()
    if age_group and age_group not in {
        "adult", "adults", "senior", "seniors", "male", "men"
    }:
        return ScopeClassification(
            competition,
            ScopeDecision.REVIEW_REQUIRED,
            f"unrecognized explicit age group {competition.age_group!r}",
            "review_unknown_age_group",
        )
    return ScopeClassification(
        competition,
        ScopeDecision.INCLUDED,
        "no women/youth/reserve/friendly exclusion signal",
        "include_male_senior_default",
    )


def competition_from_league_payload(payload: Mapping[str, Any]) -> CompetitionRef:
    """Build a source reference from ``/leagues`` response details."""

    details = payload.get("details") or {}
    if not isinstance(details, Mapping):
        raise CatalogShapeError("details must be an object")
    competition_id = _integer_id(details.get("id"))
    name = _nonempty(details.get("name", details.get("shortName")))
    if competition_id is None or name is None:
        raise CatalogShapeError("details.id and details.name are required")
    return CompetitionRef(
        competition_id=competition_id,
        name=name,
        country_code=_nonempty(details.get("country", details.get("countryCode"))),
        gender=_nonempty(details.get("gender")),
        competition_type=_nonempty(details.get("type")),
        page_url=_nonempty(details.get("pageUrl")),
        source_slug=_nonempty(details.get("seopath")),
        source_paths=("$.details",),
    )


def validate_selected_season(
    payload: Mapping[str, Any],
    expected_source_season_key: str,
    *,
    competition_id: Optional[int] = None,
) -> str:
    """Reject FotMob's silent fallback to a different/current season."""

    if not isinstance(expected_source_season_key, str) or not expected_source_season_key:
        raise ValueError("expected_source_season_key must be a non-empty exact string")
    details = payload.get("details") or {}
    overview = payload.get("overview") or {}
    selected_value = details.get("selectedSeason") if isinstance(details, Mapping) else None
    if selected_value is None and isinstance(overview, Mapping):
        selected_value = overview.get("selectedSeason", overview.get("season"))
    if selected_value is None:
        raise SelectedSeasonMismatch("response does not declare details.selectedSeason")
    selected = _source_key(selected_value, field="details.selectedSeason")
    if selected != expected_source_season_key:
        prefix = f"competition {competition_id}: " if competition_id is not None else ""
        raise SelectedSeasonMismatch(
            f"{prefix}requested exact season {expected_source_season_key!r}, "
            f"FotMob selected {selected!r}"
        )
    if competition_id is not None and isinstance(details, Mapping):
        actual_id = _integer_id(details.get("id"))
        if actual_id is not None and actual_id != int(competition_id):
            raise CatalogShapeError(
                f"requested competition {competition_id}, response identifies {actual_id}"
            )
    return selected


def parse_seasons(
    payload: Mapping[str, Any],
    competition: CompetitionRef | int | None = None,
    *,
    strict: bool = True,
) -> Tuple[SeasonRef, ...]:
    """Parse exact ``allAvailableSeasons`` labels without deriving year ranges."""

    details = payload.get("details") or {}
    if not isinstance(details, Mapping):
        raise CatalogShapeError("details must be an object")
    payload_id = _integer_id(details.get("id"))
    expected_id = competition.competition_id if isinstance(competition, CompetitionRef) else competition
    competition_id = int(expected_id) if expected_id is not None else payload_id
    if competition_id is None:
        raise CatalogShapeError("competition id is missing from details and arguments")
    if payload_id is not None and payload_id != competition_id:
        raise CatalogShapeError(
            f"requested competition {competition_id}, response identifies {payload_id}"
        )

    raw_seasons = payload.get("allAvailableSeasons") or []
    if not isinstance(raw_seasons, list):
        raise CatalogShapeError("allAvailableSeasons must be a list")
    seasons: list[str] = []
    for index, value in enumerate(raw_seasons):
        season = _source_key(value, field=f"allAvailableSeasons[{index}]")
        if season not in seasons:
            seasons.append(season)

    # Some competitions expose older/stat-enabled editions only through these
    # secondary source lists.  Union exact strings; never derive a year range.
    stats = payload.get("stats") or {}
    if isinstance(stats, Mapping):
        for index, value in enumerate(stats.get("seasonsWithLinks") or []):
            season = _source_key(value, field=f"stats.seasonsWithLinks[{index}]")
            if season not in seasons:
                seasons.append(season)
        for index, value in enumerate(stats.get("seasonStatLinks") or []):
            if not isinstance(value, Mapping) or value.get("Name") is None:
                continue
            season = _source_key(
                value["Name"], field=f"stats.seasonStatLinks[{index}].Name"
            )
            if season not in seasons:
                seasons.append(season)

    selected_value = details.get("selectedSeason")
    latest_value = details.get("latestSeason")
    selected = _source_key(selected_value, field="details.selectedSeason") if selected_value is not None else None
    latest = _source_key(latest_value, field="details.latestSeason") if latest_value is not None else None
    if not seasons:
        for value in (selected, latest):
            if value is not None and value not in seasons:
                seasons.append(value)
    if strict and selected is not None and selected not in seasons:
        raise SelectedSeasonMismatch(
            f"details.selectedSeason {selected!r} is absent from allAvailableSeasons"
        )
    if strict and latest is not None and latest not in seasons:
        raise SelectedSeasonMismatch(
            f"details.latestSeason {latest!r} is absent from allAvailableSeasons"
        )
    return tuple(
        SeasonRef(
            competition_id=competition_id,
            source_season_key=season,
            is_selected=season == selected,
            is_latest=season == latest,
            source_order=index,
        )
        for index, season in enumerate(seasons)
    )


__all__ = [
    "CatalogConflict",
    "CatalogConflictError",
    "CatalogDiscovery",
    "CatalogShapeError",
    "ClassifierHook",
    "DEFAULT_SCOPE_OVERRIDES",
    "ScopeOverride",
    "SelectedSeasonMismatch",
    "classify_competition",
    "competition_from_league_payload",
    "discover_competitions",
    "parse_seasons",
    "validate_selected_season",
]
