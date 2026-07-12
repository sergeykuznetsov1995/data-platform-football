from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_openmetadata_catalogs_canonical_reader_views_and_lineage():
    metadata = (ROOT / 'configs/openmetadata/trino_ingestion.yaml').read_text()
    lineage = (ROOT / 'configs/openmetadata/trino_lineage.yaml').read_text()
    assert 'includeViews: true' in metadata
    assert 'processViewLineage: true' in lineage
