"""Explicit source and warehouse contracts for the additional non-match sections."""

from . import agency_sections, club_sections, injury_sections, national_sections, raw_store


_MODULES = (agency_sections, club_sections, injury_sections, national_sections)
TABLES = {entity: table for module in _MODULES for entity, table in module.TABLES.items()}
BY_ENDPOINT = {endpoint: module for module in _MODULES for endpoint in module.ENDPOINT_ENTITIES}
ENTITIES = {endpoint: module.ENDPOINT_ENTITIES[endpoint] for endpoint, module in BY_ENDPOINT.items()}
JSON_ENDPOINTS = frozenset().union(*(module.JSON_ENDPOINTS for module in _MODULES))
PAGINATED = frozenset().union(*(module.PAGINATED for module in _MODULES))
PAGED_ENTITIES = frozenset(entity for endpoint in PAGINATED for entity in ENTITIES[endpoint])
CLOCK_ENTITIES = frozenset(entity for entity, table in TABLES.items()
                           if table.write_mode == 'merge' and 'observed_at' in table.natural_key)


def source_scope(endpoint, url, *, edition_id='', competition_id=''):
    module = BY_ENDPOINT.get(endpoint)
    if module is None:
        raise ValueError(f'Unknown section endpoint {endpoint!r}')
    raw_store._safe_url(url)
    return module.scope_from_url(endpoint, url, edition_id=edition_id, competition_id=competition_id)


def entity_scopes(entity, endpoint, target_scope):
    """Physical owners allowed by a source URL, including each club in a lookup."""
    if endpoint == 'national_team_reference':
        return tuple({'national_team_id': team} for team in target_scope['team_packet'].split(','))
    if endpoint == 'agency_clients':
        return (agency_sections.scope_for_entity(entity, endpoint, target_scope),)
    return (target_scope,)
