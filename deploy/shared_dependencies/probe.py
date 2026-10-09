"""Offline compatibility probe; only copied code and disposable Airflow state."""
import hashlib
import importlib
import json
from pathlib import Path
import sys

assert sys.dont_write_bytecode
assert getattr(sys, '_whoscored_runtime_startup_schema', None) == 2
assert sys._whoscored_runtime_startup_root == '/opt/airflow'
sys._load_whoscored_runtime_contract('/opt/airflow')

from airflow.models import DagBag

# Production metabase is never consulted by this offline process.
root = Path('/opt/airflow')
plan = json.loads((root / 'dependency-probe.json').read_text())
for name, expected in plan['hashes'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
for filename, dag_ids in plan['dag_files'].items():
    bag = DagBag(dag_folder=str(root / filename), include_examples=False, safe_mode=False)
    assert not bag.import_errors, bag.import_errors
    assert set(dag_ids) <= set(bag.dags), (filename, dag_ids, sorted(bag.dags))
    print(json.dumps({'dag_file': filename, 'dags': sorted(bag.dags), 'ok': True}), flush=True)
for name in ('scrapers.clubelo.daily', 'scrapers.clubelo.history',
             'scrapers.whoscored.transport', 'scripts.proxy_filter.filter_proxy',
             'scripts.fbref_proxy.filter_proxy', 'dags.scripts.prepare_sofascore_workload'):
    importlib.import_module(name)
    print(json.dumps({'import': name, 'ok': True}), flush=True)

from dags.utils import medallion_config as config
from scrapers.utils.proxy_manager import Proxy, ProxyManager
original = config.load_competitions
try:
    config.load_competitions = lambda: {'competitions': [{'id': 'probe', 'seasons': [
        {'id': '2627', 'team_count': 20, 'team_count_pending': True}]}]}
    if plan['medallion_new']:
        try:
            config.get_season_team_count('probe', '2627')
        except config.MedallionConfigError:
            pass
        else:
            raise AssertionError('pending team-count must fail closed')
    else:
        assert config.get_season_team_count('probe', '2627') == 20
finally:
    config.load_competitions = original
if plan['proxy_new']:
    # No pool or credentials loaded: test selection with one synthetic member.
    proxy = Proxy(host='127.0.0.1', port=8080)
    manager = ProxyManager.__new__(ProxyManager)
    manager._proxies = [proxy]
    manager._reactivate_expired_bans = lambda: None
    assert manager.get_proxy(excluded_http_urls={proxy.http_url}) is None
    assert manager.get_http_proxy_url(excluded_http_urls={proxy.http_url}) is None
print(json.dumps({'startup_anchor': 'passed', 'behavior': 'passed',
                  'hashes': plan['hashes']}), flush=True)
