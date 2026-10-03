import sys

assert sys.dont_write_bytecode, 'bytecode must remain disabled'
assert getattr(sys, '_whoscored_runtime_startup_schema', None) == 2
assert sys._whoscored_runtime_startup_root == '/opt/airflow'
sys._load_whoscored_runtime_contract('/opt/airflow')

import importlib
import json
import os
from pathlib import Path
import hashlib
import pyiceberg
from pyiceberg.table.snapshots import Operation, Summary

modules = [
    'scrapers.base.iceberg_writer',
    'scrapers.clubelo.daily',
    'dags.scripts.whoscored_frozen_dq',
    'scripts.proxy_filter.filter_proxy',
    'scripts.fbref_proxy.filter_proxy',
]
for module in modules:
    importlib.import_module(module)
    print(json.dumps({'import': module, 'ok': True}), flush=True)
summary = Summary(operation=Operation.APPEND, **{'dpf.operation': 'replace-identity-partition-batch'})
print(json.dumps({'pyiceberg': pyiceberg.__version__, 'summary': summary.model_dump()}), flush=True)
path = Path('/opt/airflow/scrapers/base/iceberg_writer.py')
print(json.dumps({'writer_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'inode': path.stat().st_ino, 'uid': path.stat().st_uid, 'gid': path.stat().st_gid, 'mode': oct(path.stat().st_mode & 0o7777), 'anchor': 'passed', 'pid': os.getpid()}), flush=True)
