from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]


def test_openmetadata_catalogs_canonical_reader_views_and_lineage():
    metadata = (ROOT / 'configs/openmetadata/trino_ingestion.yaml').read_text()
    lineage_path = ROOT / 'configs/openmetadata/trino_lineage.yaml'
    lineage = lineage_path.read_text()
    lineage_config = yaml.safe_load(lineage)['source']['sourceConfig']['config']
    assert 'includeViews: true' in metadata
    assert 'processViewLineage: true' in lineage
    assert lineage_config['processViewLineage'] is True
    assert lineage_config['processQueryLineage'] is False


def test_short_trino_history_disables_openmetadata_query_lineage():
    trino_config = (ROOT / 'configs/trino/config.properties').read_text().splitlines()
    lineage_config = yaml.safe_load(
        (ROOT / 'configs/openmetadata/trino_lineage.yaml').read_text()
    )['source']['sourceConfig']['config']

    assert 'query.max-history=20' in trino_config
    assert 'query.min-expire-age=10s' in trino_config
    assert lineage_config['processQueryLineage'] is False
