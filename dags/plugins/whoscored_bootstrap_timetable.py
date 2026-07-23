"""Register the serialized WhoScored bootstrap timetable with Airflow."""

from airflow.plugins_manager import AirflowPlugin

from dags.scripts.whoscored_bootstrap import AcceleratedBootstrapTimetable


class WhoScoredBootstrapTimetablePlugin(AirflowPlugin):
    name = "whoscored_accelerated_bootstrap_timetable"
    timetables = [AcceleratedBootstrapTimetable]
