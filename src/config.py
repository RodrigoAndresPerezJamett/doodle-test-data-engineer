from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"
GOLD_DIR = ROOT_DIR / "data" / "gold"
OUT_DIR = ROOT_DIR / "output"

_CONFIG = yaml.safe_load((ROOT_DIR / "config.yml").read_text())

SOURCES = _CONFIG["sources"]
THRESHOLDS = _CONFIG["thresholds"]