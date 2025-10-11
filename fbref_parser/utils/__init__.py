"""
Utility functions for FBref parsing

Contains modules for:
- URL manipulation (url_helpers.py)
- File operations (file_helpers.py)
- Squad parsing (squad_helpers.py)
"""

from .url_helpers import (
    extract_player_name_from_url,
    build_player_url,
    convert_to_all_comps_url
)
from .file_helpers import (
    normalize_name,
    ensure_directory_exists,
    get_output_path
)
from .squad_helpers import (
    extract_field_player_links,
    extract_goalkeeper_links
)

__all__ = [
    'extract_player_name_from_url',
    'build_player_url',
    'convert_to_all_comps_url',
    'normalize_name',
    'ensure_directory_exists',
    'get_output_path',
    'extract_field_player_links',
    'extract_goalkeeper_links'
]
