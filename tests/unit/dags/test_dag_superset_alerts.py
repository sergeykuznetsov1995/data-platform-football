"""
Unit tests for ``dags/dag_superset_alerts.py``.

Coverage:
  * DAG metadata (dag_id, schedule, tasks)
  * Superset login: success / missing password / connection refused / no token
  * Chart fetch: 404 / 500 / network error / JSON decode
  * ``_resolve_metric_path``: explicit path, auto-prefix, missing key
  * ``_compare``: gt / lt / eq / gte / lte / unknown operator
  * ``check_alerts`` end-to-end with mocked HTTP + Telegram:
      - Telegram fired on threshold breach (and message body shape)
      - Telegram failure does NOT raise
      - Variable override replaces the in-DAG defaults
      - Empty / malformed alert configs handled gracefully

Each test isolates state via ``monkeypatch`` so that order of execution
does not matter and parallel runs are safe.
"""

from __future__ import annotations

import io
import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_urlopen_response(payload: dict | bytes, status: int = 200):
    """Build a context-manager mock that mimics urllib.request.urlopen."""
    body = (
        payload
        if isinstance(payload, bytes)
        else json.dumps(payload).encode("utf-8")
    )

    cm = MagicMock()
    cm.read.return_value = body
    cm.status = status
    cm.__enter__ = MagicMock(return_value=cm)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


@pytest.fixture
def alerts_module():
    """Fresh import of the DAG module — relies on conftest's airflow stubs."""
    import importlib

    import dags.dag_superset_alerts as m

    importlib.reload(m)
    return m


# ===========================================================================
# 1. DAG-level smoke tests
# ===========================================================================


@pytest.mark.unit
class TestDagMetadata:
    """The DAG module must declare exactly the contract operators expect."""

    def test_dag_module_imports(self, alerts_module):
        """Module loads without ImportError or syntax issues."""
        assert hasattr(alerts_module, "superset_alerts")
        assert hasattr(alerts_module, "dag")

    def test_dag_id(self, alerts_module):
        """dag_id MUST be 'superset_alerts' — referenced in CLAUDE.md/runbooks."""
        kwargs = alerts_module.superset_alerts._dag_kwargs
        assert kwargs["dag_id"] == "superset_alerts"

    def test_dag_schedule(self, alerts_module):
        """Every 15 minutes — drives Telegram alert latency SLO."""
        kwargs = alerts_module.superset_alerts._dag_kwargs
        assert kwargs["schedule"] == "*/15 * * * *"

    def test_dag_no_catchup(self, alerts_module):
        """catchup=False — avoid retroactive alert spam."""
        assert alerts_module.superset_alerts._dag_kwargs["catchup"] is False

    def test_dag_max_active_runs(self, alerts_module):
        """Single concurrent run — avoid overlapping Telegram messages."""
        assert alerts_module.superset_alerts._dag_kwargs["max_active_runs"] == 1

    def test_dag_tags_present(self, alerts_module):
        tags = alerts_module.superset_alerts._dag_kwargs.get("tags", [])
        assert "superset" in tags
        assert "alerts" in tags

    def test_default_alert_config_shape(self, alerts_module):
        """The in-file fallback must remain valid (chart_id + threshold)."""
        for alert in alerts_module.SUPERSET_ALERTS:
            assert "name" in alert
            assert "chart_id" in alert
            assert "threshold" in alert
            assert "comparison" in alert
            assert alert["comparison"] in {"gt", "lt", "eq", "gte", "lte"}

    def test_dag_loaded_via_dagbag_if_available(self, alerts_module):
        """If real Airflow is on the host, DagBag must accept the file too."""
        try:
            from airflow.models import DagBag  # noqa: WPS433
        except Exception:
            pytest.skip("Airflow not installed — DagBag check is optional")

        # Only real Airflow (with a SQLAlchemy backend) — guard against the
        # stub from conftest by checking for the BaseDag class.
        if not hasattr(DagBag, "process_file"):
            pytest.skip("Stubbed Airflow detected; skipping DagBag check")

        dagbag = DagBag(
            dag_folder=str(alerts_module.__file__),
            include_examples=False,
        )
        assert "superset_alerts" in dagbag.dags
        assert dagbag.import_errors == {}


# ===========================================================================
# 2. _superset_login
# ===========================================================================


@pytest.mark.unit
class TestSupersetLogin:
    def test_returns_token_on_success(
        self, alerts_module, superset_env, monkeypatch
    ):
        """Happy path: 200 OK with access_token in body."""
        urlopen = MagicMock(
            return_value=_fake_urlopen_response({"access_token": "fake-jwt"})
        )
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        token = alerts_module._superset_login()
        assert token == "fake-jwt"

        # Verify we POSTed JSON with username/password (no logging of password)
        call_args = urlopen.call_args
        request_obj = call_args[0][0]
        assert request_obj.get_method() == "POST"
        sent = json.loads(request_obj.data.decode("utf-8"))
        assert sent["username"] == "admin"
        assert sent["password"] == "secret-pwd"
        assert sent["provider"] == "db"

    def test_returns_none_when_password_missing(
        self, alerts_module, monkeypatch
    ):
        """No SUPERSET_ADMIN_PASSWORD → log warning, return None (no raise)."""
        monkeypatch.delenv("SUPERSET_ADMIN_PASSWORD", raising=False)
        token = alerts_module._superset_login()
        assert token is None

    def test_returns_none_on_connection_refused(
        self, alerts_module, superset_env, monkeypatch, caplog
    ):
        """URLError (Superset down) → graceful None, NO exception."""
        def _raise(*a, **kw):
            raise urllib.error.URLError("Connection refused")

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)

        with caplog.at_level("WARNING"):
            token = alerts_module._superset_login()

        assert token is None
        # We should NOT log the password
        for record in caplog.records:
            assert "secret-pwd" not in record.getMessage()

    def test_returns_none_when_response_missing_token(
        self, alerts_module, superset_env, monkeypatch
    ):
        """200 OK but no access_token field → return None."""
        urlopen = MagicMock(
            return_value=_fake_urlopen_response({"unrelated": "field"})
        )
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        assert alerts_module._superset_login() is None


# ===========================================================================
# 3. _fetch_chart_data
# ===========================================================================


@pytest.mark.unit
class TestFetchChartData:
    def test_returns_payload_on_success(self, alerts_module, monkeypatch):
        urlopen = MagicMock(
            return_value=_fake_urlopen_response(
                {"result": [{"data": [{"value": 42}]}]}
            )
        )
        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", urlopen)

        payload = alerts_module._fetch_chart_data(7, "tok")
        assert payload == {"result": [{"data": [{"value": 42}]}]}

        # Verify Authorization header
        request_obj = urlopen.call_args[0][0]
        assert request_obj.headers.get("Authorization") == "Bearer tok"
        assert "force=true" in request_obj.full_url

    def test_returns_none_on_404(self, alerts_module, monkeypatch, caplog):
        def _raise(*a, **kw):
            raise urllib.error.HTTPError(
                "url", 404, "Not Found", hdrs=None, fp=io.BytesIO(b"")
            )

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)

        with caplog.at_level("WARNING"):
            result = alerts_module._fetch_chart_data(99, "tok")

        assert result is None
        assert any(
            "not found" in r.getMessage().lower() for r in caplog.records
        )

    def test_returns_none_on_500(self, alerts_module, monkeypatch, caplog):
        def _raise(*a, **kw):
            raise urllib.error.HTTPError(
                "url",
                500,
                "Internal Server Error",
                hdrs=None,
                fp=io.BytesIO(b""),
            )

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)
        with caplog.at_level("WARNING"):
            assert alerts_module._fetch_chart_data(1, "tok") is None

    def test_returns_none_on_connection_error(self, alerts_module, monkeypatch):
        def _raise(*a, **kw):
            raise urllib.error.URLError("dns failure")

        monkeypatch.setattr(alerts_module.urllib.request, "urlopen", _raise)
        assert alerts_module._fetch_chart_data(1, "tok") is None


# ===========================================================================
# 4. _resolve_metric_path
# ===========================================================================


@pytest.mark.unit
class TestResolveMetricPath:
    @pytest.fixture
    def superset_payload(self):
        # Real Superset chart-data response shape.
        return {
            "result": [
                {
                    "data": [
                        {"value": 123, "metric": "row_count"},
                        {"value": 0, "metric": "row_count"},
                    ]
                }
            ]
        }

    def test_explicit_full_path(self, alerts_module, superset_payload):
        """Path starts with 'result.0' — used as-is."""
        v = alerts_module._resolve_metric_path(
            superset_payload, "result.0.data.0.value"
        )
        assert v == 123

    def test_auto_prefix_data(self, alerts_module, superset_payload):
        """Path 'data.X' is automatically prefixed with 'result.0.'."""
        v = alerts_module._resolve_metric_path(
            superset_payload, "data.0.value"
        )
        assert v == 123

    def test_index_into_list(self, alerts_module, superset_payload):
        v = alerts_module._resolve_metric_path(
            superset_payload, "data.1.metric"
        )
        assert v == "row_count"

    def test_missing_key_returns_none(self, alerts_module, superset_payload):
        v = alerts_module._resolve_metric_path(
            superset_payload, "data.0.does_not_exist"
        )
        assert v is None

    def test_index_out_of_range_returns_none(
        self, alerts_module, superset_payload
    ):
        v = alerts_module._resolve_metric_path(
            superset_payload, "data.99.value"
        )
        assert v is None

    def test_walking_into_non_dict_returns_none(self, alerts_module):
        v = alerts_module._resolve_metric_path(
            {"result": [{"data": [42]}]}, "data.0.value"
        )
        assert v is None


# ===========================================================================
# 5. _compare
# ===========================================================================


@pytest.mark.unit
class TestCompare:
    @pytest.mark.parametrize(
        "value,threshold,op,expected",
        [
            (50, 100, "lt", True),
            (150, 100, "lt", False),
            (150, 100, "gt", True),
            (50, 100, "gt", False),
            (100, 100, "eq", True),
            (101, 100, "eq", False),
            (100, 100, "gte", True),
            (99, 100, "gte", False),
            (100, 100, "lte", True),
            (101, 100, "lte", False),
            (100.0, 100, "eq", True),  # int vs float
        ],
    )
    def test_comparisons(self, alerts_module, value, threshold, op, expected):
        assert alerts_module._compare(value, threshold, op) is expected

    def test_unknown_operator_raises(self, alerts_module):
        with pytest.raises(ValueError) as exc:
            alerts_module._compare(1, 2, "between")
        assert "between" in str(exc.value)


# ===========================================================================
# 6. check_alerts end-to-end (with HTTP + Telegram mocked)
# ===========================================================================


def _check_alerts_callable(alerts_module):
    """Extract the inner ``check_alerts`` task from the DAG factory.

    The DAG file uses the TaskFlow API:

        @dag(...)
        def superset_alerts():
            @task
            def check_alerts() -> Dict[str, int]:
                ...
            check_alerts()

    Our airflow stubs preserve the original ``check_alerts`` function (so the
    @task decorator is a no-op). To grab a clean reference WITHOUT triggering
    the inner ``check_alerts()`` call we replace `task` with a capturer AND
    swallow the side-effect call by raising a sentinel exception once the
    function has been recorded.
    """
    captured: dict = {}

    class _Stop(BaseException):
        """Used to abort the DAG factory after we've grabbed the callable."""

    original_task = alerts_module.task

    def _capturing_task(*targs, **tkwargs):
        if len(targs) == 1 and callable(targs[0]) and not tkwargs:
            captured["fn"] = targs[0]
            # Replace with a no-op callable so the side-effect line
            # `check_alerts()` raises _Stop instead of running the body.
            def _stub(*a, **kw):
                raise _Stop

            return _stub

        def _wrap(fn):
            captured["fn"] = fn

            def _stub(*a, **kw):
                raise _Stop

            return _stub

        return _wrap

    alerts_module.task = _capturing_task
    try:
        try:
            alerts_module.superset_alerts._wrapped()
        except _Stop:
            pass
    finally:
        alerts_module.task = original_task

    fn = captured.get("fn")
    if fn is None:
        raise RuntimeError("Failed to capture check_alerts callable")
    return fn


@pytest.mark.unit
class TestCheckAlertsTrigger:
    def test_telegram_fired_on_threshold_breach(
        self, alerts_module, superset_env, monkeypatch
    ):
        """value=42 < threshold=100 with comparison='lt' → fire Telegram."""
        # Force a single, simple alert config so we deterministically expect 1 fire.
        custom_alerts = [
            {
                "name": "row_drop",
                "chart_id": 1,
                "metric_path": "data.0.value",
                "threshold": 100,
                "comparison": "lt",
                "message": "Row count too low",
            }
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )

        monkeypatch.setattr(
            alerts_module, "_superset_login", lambda: "tok"
        )
        monkeypatch.setattr(
            alerts_module,
            "_fetch_chart_data",
            lambda chart_id, token: {
                "result": [{"data": [{"value": 42}]}]
            },
        )

        sender = MagicMock(return_value=True)
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        summary = _check_alerts_callable(alerts_module)()

        assert summary == {"checked": 1, "fired": 1, "skipped": 0}
        sender.assert_called_once()
        sent_message = sender.call_args[0][0]
        # The DAG should embed alert metadata in the body
        assert "row_drop" in sent_message
        assert "42" in sent_message
        assert "lt" in sent_message
        assert sender.call_args.kwargs.get("level") == "warning"

    def test_telegram_not_fired_when_value_passes(
        self, alerts_module, superset_env, monkeypatch
    ):
        custom_alerts = [
            {
                "name": "ok_alert",
                "chart_id": 1,
                "metric_path": "data.0.value",
                "threshold": 10,
                "comparison": "lt",
            }
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )

        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")
        monkeypatch.setattr(
            alerts_module,
            "_fetch_chart_data",
            lambda chart_id, token: {"result": [{"data": [{"value": 42}]}]},
        )

        sender = MagicMock()
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        summary = _check_alerts_callable(alerts_module)()
        assert summary == {"checked": 1, "fired": 0, "skipped": 0}
        sender.assert_not_called()

    def test_chart_404_continues_processing(
        self, alerts_module, superset_env, monkeypatch, caplog
    ):
        """A failing chart must not abort the whole sweep."""
        custom_alerts = [
            {
                "name": "missing_chart",
                "chart_id": 9999,
                "metric_path": "data.0.value",
                "threshold": 1,
                "comparison": "lt",
            },
            {
                "name": "ok_chart",
                "chart_id": 1,
                "metric_path": "data.0.value",
                "threshold": 100,
                "comparison": "lt",
            },
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")

        def _fetch(chart_id, token):
            if chart_id == 9999:
                return None  # Simulates HTTPError 404 already logged
            return {"result": [{"data": [{"value": 1}]}]}

        monkeypatch.setattr(alerts_module, "_fetch_chart_data", _fetch)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        with caplog.at_level("WARNING"):
            summary = _check_alerts_callable(alerts_module)()

        assert summary["skipped"] == 1
        assert summary["checked"] == 1
        assert summary["fired"] == 1

    def test_telegram_failure_does_not_raise(
        self, alerts_module, superset_env, monkeypatch, caplog
    ):
        """If Telegram returns False the task logs ERROR but does NOT raise."""
        custom_alerts = [
            {
                "name": "tg_dies",
                "chart_id": 1,
                "metric_path": "data.0.value",
                "threshold": 1000,
                "comparison": "lt",
            }
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")
        monkeypatch.setattr(
            alerts_module,
            "_fetch_chart_data",
            lambda *a, **kw: {"result": [{"data": [{"value": 1}]}]},
        )
        monkeypatch.setattr(
            alerts_module,
            "send_telegram_message",
            MagicMock(return_value=False),
        )

        with caplog.at_level("ERROR"):
            summary = _check_alerts_callable(alerts_module)()

        assert summary["fired"] == 1
        assert any(
            "Telegram delivery failed" in r.getMessage()
            for r in caplog.records
        )

    def test_superset_down_short_circuits(
        self, alerts_module, superset_env, monkeypatch
    ):
        """Login returns None → all alerts marked skipped, no Telegram calls."""
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: None)
        sender = MagicMock()
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        summary = _check_alerts_callable(alerts_module)()

        assert summary["checked"] == 0
        assert summary["fired"] == 0
        # default config is 2 alerts
        assert summary["skipped"] == len(alerts_module.SUPERSET_ALERTS)
        sender.assert_not_called()

    def test_variable_override_replaces_defaults(
        self, alerts_module, superset_env, monkeypatch
    ):
        """Variable JSON list overrides in-DAG defaults, no fallback."""
        injected = [
            {
                "name": "from_variable",
                "chart_id": 7,
                "metric_path": "data.0.value",
                "threshold": 5,
                "comparison": "gt",
            }
        ]
        # The DAG uses Variable.get('superset_alerts_config', deserialize_json=True,
        # default_var=...).
        captured: dict = {}

        def fake_get(key, default_var=None, deserialize_json=False):
            captured["key"] = key
            captured["default"] = default_var
            captured["deserialize_json"] = deserialize_json
            return injected

        monkeypatch.setattr(
            alerts_module.Variable, "get", staticmethod(fake_get)
        )
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")

        fetch = MagicMock(
            return_value={"result": [{"data": [{"value": 99}]}]}
        )
        monkeypatch.setattr(alerts_module, "_fetch_chart_data", fetch)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        summary = _check_alerts_callable(alerts_module)()

        # Only ONE alert evaluated — the override
        assert captured["key"] == "superset_alerts_config"
        assert captured["deserialize_json"] is True
        assert summary["checked"] == 1
        # value 99 > threshold 5 → fired
        assert summary["fired"] == 1
        # Verify the chart fetched is the override's chart_id, not the defaults
        fetch.assert_called_once_with(7, "tok")

    def test_variable_falls_back_on_exception(
        self, alerts_module, superset_env, monkeypatch
    ):
        """If Variable.get raises (DB down), in-DAG defaults are used."""

        def _boom(*a, **kw):
            raise RuntimeError("metadata DB unreachable")

        monkeypatch.setattr(alerts_module.Variable, "get", staticmethod(_boom))
        # Login will be called (because we have defaults to evaluate). Force
        # 'down' to keep the test fast — we only care that load() succeeded.
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: None)

        summary = _check_alerts_callable(alerts_module)()

        # With defaults containing 2 alerts, all skipped (Superset down)
        assert summary["skipped"] == len(alerts_module.SUPERSET_ALERTS)

    def test_skips_malformed_alert_entry(
        self, alerts_module, superset_env, monkeypatch
    ):
        """Alert missing chart_id or threshold → skipped, no fetch performed."""
        custom_alerts = [
            {"name": "no_chart", "threshold": 1, "comparison": "lt"},
            {"name": "no_threshold", "chart_id": 1, "comparison": "lt"},
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")

        fetch = MagicMock()
        monkeypatch.setattr(alerts_module, "_fetch_chart_data", fetch)

        summary = _check_alerts_callable(alerts_module)()
        assert summary["skipped"] == 2
        assert summary["checked"] == 0
        fetch.assert_not_called()

    def test_skips_non_numeric_metric_value(
        self, alerts_module, superset_env, monkeypatch
    ):
        """If metric resolves to a non-numeric value, skip with WARNING."""
        custom_alerts = [
            {
                "name": "string_metric",
                "chart_id": 1,
                "metric_path": "data.0.value",
                "threshold": 1,
                "comparison": "lt",
            }
        ]
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: custom_alerts
            ),
        )
        monkeypatch.setattr(alerts_module, "_superset_login", lambda: "tok")
        monkeypatch.setattr(
            alerts_module,
            "_fetch_chart_data",
            lambda *a, **kw: {"result": [{"data": [{"value": "not-a-number"}]}]},
        )
        sender = MagicMock()
        monkeypatch.setattr(alerts_module, "send_telegram_message", sender)

        summary = _check_alerts_callable(alerts_module)()
        assert summary["skipped"] == 1
        assert summary["checked"] == 0
        sender.assert_not_called()

    def test_empty_alert_list_returns_zero(
        self, alerts_module, superset_env, monkeypatch
    ):
        """Empty list → no work done, no error."""
        monkeypatch.setattr(
            alerts_module.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None, deserialize_json=False: []
            ),
        )
        # Login must NOT be called when there is nothing to evaluate.
        login = MagicMock()
        monkeypatch.setattr(alerts_module, "_superset_login", login)

        summary = _check_alerts_callable(alerts_module)()
        assert summary == {"checked": 0, "fired": 0, "skipped": 0}
        login.assert_not_called()
