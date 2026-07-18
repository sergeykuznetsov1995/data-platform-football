#!/usr/bin/env python3
"""Create and verify the platform S3 bucket on a fresh SeaweedFS deployment."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

from pyarrow import fs


_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def initialize_bucket(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    bucket = values.get("ICEBERG_WAREHOUSE", "").strip()
    if not _BUCKET_RE.fullmatch(bucket):
        raise ValueError("ICEBERG_WAREHOUSE must be a valid non-empty S3 bucket name")

    access_key = values.get("S3_ACCESS_KEY", "").strip()
    secret_key = values.get("S3_SECRET_KEY", "").strip()
    if not access_key or not secret_key:
        raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY are required")

    endpoint = values.get("S3_ENDPOINT", "seaweedfs:8333").strip()
    scheme = values.get("S3_SCHEME", "http").strip()
    if scheme not in {"http", "https"}:
        raise ValueError("S3_SCHEME must be http or https")

    store = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint,
        scheme=scheme,
        region=values.get("S3_REGION", "us-east-1"),
        allow_bucket_creation=True,
    )
    info = store.get_file_info(bucket)
    if info.type is fs.FileType.NotFound:
        store.create_dir(bucket, recursive=False)
        info = store.get_file_info(bucket)
    if info.type is not fs.FileType.Directory:
        raise RuntimeError(f"S3 bucket {bucket!r} was not created or is not readable")
    return bucket


def main() -> int:
    bucket = initialize_bucket()
    print(f"S3 bucket is ready: {bucket}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
