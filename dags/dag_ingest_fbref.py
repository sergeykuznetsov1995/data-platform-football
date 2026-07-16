"""Scheduled production FBref current refresh."""

from airflow import DAG

from utils.fbref_current_dag_factory import (
    CURRENT_MAX_BATCHES as _CURRENT_MAX_BATCHES,
    PAGE_KINDS as _PAGE_KINDS,
    build_fbref_current_dag,
)


CURRENT_MAX_BATCHES = _CURRENT_MAX_BATCHES
PAGE_KINDS = _PAGE_KINDS
dag: DAG = build_fbref_current_dag(bootstrap_only=False)


__all__ = ["dag"]
