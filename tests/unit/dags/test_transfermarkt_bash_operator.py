"""Real-Airflow regressions; the host's lightweight DAG stub cannot render."""
from datetime import datetime, timezone
import json
import sys
import unittest

from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.transfermarkt_bash_operator import TransfermarktBashOperator


@unittest.skipUnless(hasattr(BashOperator, 'render_template_fields'), 'requires real Airflow')
class TestTransfermarktBashEnvironment(unittest.TestCase):
    def make_task(self, **kwargs):
        with DAG(
            'tm_literal_environment', start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            schedule=None, render_template_as_native_obj=True,
        ) as dag:
            task = TransfermarktBashOperator(task_id='child', **kwargs)
        return task, {'dag': dag, 'task': task, 'run_id': 'offline-test', 'params': task.params}

    def test_native_render_keeps_numbers_json_and_signed_payload_literal(self):
        original = {
            'TM_READER_REVISION': '7', 'TM_CURRENT_JOB': '/tmp/current-job.json',
            'TM_SCOPE_PAYLOAD_JSON': json.dumps({'season': 2025, 'label': '{{ 3 + 4 }}'}),
            'TM_POLICY_HASH': '0' * 64, 'TM_LABEL': '{{ params.value }}',
        }
        task, context = self.make_task(env=dict(original), bash_command='echo {{ params.value }}', params={'value': 42})
        task.render_template_fields(context)
        self.assertEqual(task.env, original)
        self.assertEqual(task.bash_command, 'echo 42')
        # Popen's actual process boundary must receive text, rather than only
        # passing an assertion about the declared template_fields.
        environment = task.get_env(context)
        result = task.subprocess_hook.run_command(
            command=[sys.executable, '-c', 'import os,json; print(json.dumps({k:os.environ[k] for k in '+repr(list(original))+'}))'],
            env=environment,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(json.loads(result.output), original)

    def test_mapped_child_preserves_concrete_environment_and_timeout(self):
        from datetime import timedelta
        from airflow.models.mappedoperator import MappedOperator
        from airflow.serialization.serialized_objects import SerializedDAG

        environment = {'TM_READER_REVISION': '7', 'TM_SCOPE_PAYLOAD_JSON': '{"season":2025}'}
        with DAG(
            'tm_mapped_literal_environment', start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            schedule=None, render_template_as_native_obj=True,
        ) as dag:
            mapped = TransfermarktBashOperator.partial(
                task_id='child', bash_command='true', append_env=True,
            ).expand_kwargs([{'env': environment, 'execution_timeout': timedelta(minutes=45)}], strict=False)
        self.assertIsInstance(mapped, MappedOperator)
        # Use the framework's real unmapping and serialization APIs.
        concrete = mapped.unmap({'env': environment, 'execution_timeout': timedelta(minutes=45)})
        concrete.render_template_fields({'dag': dag, 'task': concrete, 'run_id': 'offline-mapped'})
        self.assertEqual(concrete.env, environment)
        self.assertEqual(concrete.execution_timeout, timedelta(minutes=45))
        restored = SerializedDAG.from_dict(SerializedDAG.to_dict(dag)).get_task('child')
        self.assertNotIn('env', restored.template_fields)
        self.assertIn('bash_command', restored.template_fields)


if __name__ == '__main__':
    unittest.main()
