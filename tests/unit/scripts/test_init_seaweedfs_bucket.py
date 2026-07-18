from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyarrow import fs

from scripts.init_seaweedfs_bucket import initialize_bucket


ENV = {
    "ICEBERG_WAREHOUSE": "football",
    "S3_ACCESS_KEY": "access",
    "S3_SECRET_KEY": "secret",
    "S3_ENDPOINT": "seaweedfs:8333",
    "S3_SCHEME": "http",
}


def test_existing_bucket_is_verified_without_mutation() -> None:
    store = MagicMock()
    store.get_file_info.return_value = MagicMock(type=fs.FileType.Directory)

    with patch("scripts.init_seaweedfs_bucket.fs.S3FileSystem", return_value=store):
        assert initialize_bucket(ENV) == "football"

    store.create_dir.assert_not_called()


def test_missing_bucket_is_created_and_read_back() -> None:
    store = MagicMock()
    store.get_file_info.side_effect = [
        MagicMock(type=fs.FileType.NotFound),
        MagicMock(type=fs.FileType.Directory),
    ]

    with patch(
        "scripts.init_seaweedfs_bucket.fs.S3FileSystem", return_value=store
    ) as filesystem:
        assert initialize_bucket(ENV) == "football"

    assert filesystem.call_args.kwargs["allow_bucket_creation"] is True
    store.create_dir.assert_called_once_with("football", recursive=False)


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("ICEBERG_WAREHOUSE", "", "valid non-empty"),
        ("ICEBERG_WAREHOUSE", "../unsafe", "valid non-empty"),
        ("S3_ACCESS_KEY", "", "S3_ACCESS_KEY"),
        ("S3_SECRET_KEY", "", "S3_ACCESS_KEY"),
        ("S3_SCHEME", "ftp", "http or https"),
    ],
)
def test_invalid_configuration_fails_closed(key: str, value: str, message: str) -> None:
    env = {**ENV, key: value}
    with pytest.raises(ValueError, match=message):
        initialize_bucket(env)


def test_non_directory_result_fails_closed() -> None:
    store = MagicMock()
    store.get_file_info.return_value = MagicMock(type=fs.FileType.File)

    with patch("scripts.init_seaweedfs_bucket.fs.S3FileSystem", return_value=store):
        with pytest.raises(RuntimeError, match="not created or is not readable"):
            initialize_bucket(ENV)
