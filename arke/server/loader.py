"""Loader — turns raw files from digest/ into (Doc, text) pairs.

Dispatcher uses file extension. Supported: .txt .md .pdf .docx .msg
Unsupported files are skipped silently — unknown formats are normal in digest/.
"""
import hashlib
import logging
from pathlib import Path

from .types import Doc

logger = logging.getLogger(__name__)


def load_digest(digest_path: Path) -> list[tuple[Doc, str]]:
    """Parse all supported files under digest_path. Returns (Doc, text) pairs."""
    results: list[tuple[Doc, str]] = []
    for path in sorted(digest_path.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        result = load_file(path, root=digest_path)
        if result is not None:
            results.append(result)
    return results


def load_file(path: Path, root: Path | None = None) -> tuple[Doc, str] | None:
    """Parse a single file. Returns (Doc, text) or None if unsupported/empty."""
    suffix = path.suffix.lower()

    match suffix:
        case ".txt" | ".md":
            text = _load_txt(path)
        case ".pdf":
            text = _load_pdf(path)
        case ".docx":
            text = _load_docx(path)
        case ".msg":
            text = _load_msg(path)
        case _:
            logger.debug("skipping unsupported file: %s", path.name)
            return None

    if not text or not text.strip():
        return None

    source = str(path.relative_to(root)) if root else path.name
    stat = path.stat()
    doc = Doc(
        id=_content_id(path),
        source=source,
        created=int(stat.st_ctime),
        modified=int(stat.st_mtime),
        metadata={"filename": path.name, "suffix": suffix},
    )
    return doc, text.strip()


def _content_id(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _load_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _load_pdf(path: Path) -> str:
    import pdfplumber
    pages: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
    return "\n\n".join(pages)


def _load_docx(path: Path) -> str:
    from docx import Document
    doc = Document(path)
    return "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())


def _load_msg(path: Path) -> str:
    import extract_msg
    with extract_msg.Message(str(path)) as msg:
        parts: list[str] = []
        if msg.subject:
            parts.append(f"Subject: {msg.subject}")
        if msg.body:
            parts.append(msg.body)
        return "\n\n".join(parts)
