"""Only source-specific timetable registration; no other source imports."""

from airflow.plugins_manager import AirflowPlugin

from dags.utils.transfermarkt_current_timetable import TransfermarktCurrentTimetable


class TransfermarktCurrentTimetablePlugin(AirflowPlugin):
    name = "transfermarkt_current_timetable"
    timetables = [TransfermarktCurrentTimetable]
