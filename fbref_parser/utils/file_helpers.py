"""
File operation utilities for FBref parser

This module provides functions for:
- Normalizing player names for filenames
- Creating directories
- Generating output file paths
"""

import os
import re


def normalize_name(name: str) -> str:
    """
    Normalize player name for filesystem-safe filename

    Removes non-alphabetic characters and replaces spaces with underscores.

    Args:
        name: Player name to normalize

    Returns:
        Normalized lowercase name with underscores
    """
    # Remove all non-alphabetic characters
    clean_name = re.sub(r'[^\w\s-]', '', name.strip())
    # Replace spaces with underscores
    clean_name = re.sub(r'[\s-]+', '_', clean_name)
    return clean_name.lower()


def ensure_directory_exists(directory_path: str) -> None:
    """
    Create directory if it doesn't exist

    Args:
        directory_path: Path to directory
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"📁 Создана папка: {directory_path}")


def get_output_path(player_name: str, output_dir: str = None,
                    simple_filename: bool = False,
                    suffix: str = "_all_competitions") -> str:
    """
    Generate output file path for player data

    Args:
        player_name: Player name
        output_dir: Output directory (optional)
        simple_filename: If True, uses simple name without suffix
        suffix: Suffix to add to filename (default: "_all_competitions")

    Returns:
        Complete output file path
    """
    normalized_name = normalize_name(player_name)

    if simple_filename:
        filename = f"{normalized_name}.csv"
    else:
        filename = f"{normalized_name}{suffix}.csv"

    if output_dir:
        ensure_directory_exists(output_dir)
        return os.path.join(output_dir, filename)
    else:
        return f"/root/data_platform/{filename}"
