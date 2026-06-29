"""
Root conftest.py for pytest.
Configures Python path for all tests.
"""

import sys
from pathlib import Path

# Add project root and dags folder to Python path once, before any test module is
# imported. Tests import production code both as a package (``import dags.utils...``,
# resolved via project root) and as top-level modules (``import dag_ingest_fbref``,
# resolved via the dags folder). Consolidating here lets individual test modules and
# conftests drop their own module-level sys.path.insert calls (see issue #256).
project_root = Path(__file__).parent
for _path in (project_root, project_root / "dags"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
