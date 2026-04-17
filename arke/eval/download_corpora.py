"""Download eval corpora. Run once to populate ~/.arke/data/.

Usage:
    python -m arke.eval.download_corpora

Corpora:
    legalbench-rag  — ~87 MB, legal QA benchmark
    (more to be added)
"""
import io
import sys
import zipfile
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

DATA_DIR = Path.home() / ".arke" / "data"

CORPORA = {
    "legalbench-rag": {
        "url": "https://www.dropbox.com/scl/fo/r7xfa5i3hdsbxex1w6amw/AID389Olvtm-ZLTKAPrw6k4?rlkey=5n8zrbk4c08lbit3iiexofmwg&st=0hu354cq&dl=1",
        "dest": DATA_DIR / "legalbench-rag",
        "check": "corpus/*.txt",
        "size": "~87 MB",
    },
}


def download(name: str) -> None:
    corpus = CORPORA[name]
    dest: Path = corpus["dest"]
    check_glob: str = corpus["check"]

    if dest.exists() and any(dest.rglob(check_glob.split("/")[-1])):
        print(f"{name}: already at {dest}")
        return

    print(f"{name}: downloading {corpus['size']}...")
    try:
        raw = urlopen(corpus["url"]).read()
    except URLError as exc:
        raise RuntimeError(f"{name}: download failed: {exc}") from exc

    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        zf.extractall(dest)

    count = sum(1 for _ in dest.rglob("*.txt"))
    print(f"{name}: extracted {count} files to {dest}")


def main() -> None:
    for name in CORPORA:
        try:
            download(name)
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
