"""Exercise the host hook without importing the live watchdog or sending a report."""

import ast
import json
from pathlib import Path

import pytest


PATCH = (
    Path(__file__).resolve().parents[3]
    / "dags/scripts/transfermarkt-morning-report.patch"
)


@pytest.fixture
def render():
    # Parse the target side of the hunk, including unchanged context lines.
    hunk = PATCH.read_text().split("@@\n", 1)[1]
    target = "\n".join(
        line[1:] for line in hunk.splitlines() if line.startswith((" ", "+"))
    )
    start = target.index("def transfermarkt_measure_dq(")
    hook = ast.parse(target[start:]).body[0]

    def call(output):
        calls = []

        def run(command, *, timeout):
            calls.append((command, timeout))
            return output

        namespace = {"json": json, "run": run}
        exec(
            compile(ast.Module(body=[hook], type_ignores=[]), str(PATCH), "exec"),
            namespace,
        )
        lines = namespace["transfermarkt_measure_dq"]()
        assert len(calls) == 1
        assert calls[0][0].split() == [
            "docker",
            "exec",
            "--workdir",
            "/tmp",
            "transfermarkt-airflow-scheduler",
            "python",
            "-B",
            "/opt/airflow/dags/scripts/transfermarkt_morning_report.py",
        ]
        return lines

    return call


def test_renderer_preserves_measure_dq_and_policy_warnings(render):
    result = {"summary": "охват 1/3; Bronze DQ RED\nТРЕВОГА: истекает политика"}
    assert render(json.dumps(result)) == [
        "• охват 1/3; Bronze DQ RED",
        "• ТРЕВОГА: истекает политика",
    ]


@pytest.mark.parametrize(
    "output", ["", "not json", "[]", "{}", '{"summary":2}', '{"summary":" "}']
)
def test_unavailable_scheduler_is_reported_without_interrupting_other_sources(
    render, output
):
    assert render(output) == [
        "• Transfermarkt: измеритель не прочитан; Bronze DQ UNKNOWN ⚠️"
    ]
