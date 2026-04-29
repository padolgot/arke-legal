"""Anchor corpora-pipeline paths to the umbrella .env location.

Layout assumed:
    <umbrella>/.env
    <umbrella>/<repo>/arke/corpora/_paths.py
    <umbrella>/corpora/                       ← scrape/output data root

Find .env via dotenv.find_dotenv() walking up from this file. Fall back to
__file__-derived umbrella if .env is missing (fresh clone, no creds yet).
"""
from pathlib import Path

from dotenv import find_dotenv

_env = find_dotenv(usecwd=False)
UMBRELLA = Path(_env).resolve().parent if _env else Path(__file__).resolve().parents[3]
DATA = UMBRELLA / "corpora"
ENV_FILE = UMBRELLA / ".env"
