import sys
from pathlib import Path

SAM_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

for package_dir in (SAM_ROOT / "cartelera", SAM_ROOT / "duckdb_handler"):
    if str(package_dir) not in sys.path:
        sys.path.insert(0, str(package_dir))
