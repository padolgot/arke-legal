"""LegalBench-RAG downloader. Used by digest when DATA_PATH is the Dropbox URL."""
import io
import zipfile
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

LEGALBENCH_URL = "https://www.dropbox.com/scl/fo/r7xfa5i3hdsbxex1w6amw/AID389Olvtm-ZLTKAPrw6k4?rlkey=5n8zrbk4c08lbit3iiexofmwg&st=0hu354cq&dl=1"
DATA_DIR = Path.home() / ".arke" / "data" / "legalbench-rag"


def download_legalbench() -> str:
    """Download and extract LegalBench-RAG corpus. Returns path to corpus dir.
    Idempotent — skips download if already present."""
    corpus_dir = DATA_DIR / "corpus"
    if corpus_dir.exists() and any(corpus_dir.rglob("*.txt")):
        print(f"legalbench-rag already at {corpus_dir}")
        return str(corpus_dir)

    print("downloading legalbench-rag (~87 MB)...")
    try:
        raw = urlopen(LEGALBENCH_URL).read()
    except URLError as exc:
        raise RuntimeError(f"failed to download legalbench-rag: {exc}") from exc

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        zf.extractall(DATA_DIR)

    if not corpus_dir.exists():
        raise RuntimeError(f"expected {corpus_dir} after extraction, not found")

    count = sum(1 for _ in corpus_dir.rglob("*.txt"))
    print(f"extracted {count} documents to {corpus_dir}")
    return str(corpus_dir)
