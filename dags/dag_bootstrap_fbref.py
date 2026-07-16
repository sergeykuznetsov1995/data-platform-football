"""Manual-only non-publishing FBref current bootstrap."""

from airflow import DAG

from utils.fbref_current_dag_factory import build_fbref_current_dag


dag: DAG = build_fbref_current_dag(bootstrap_only=True)


__all__ = ["dag"]
