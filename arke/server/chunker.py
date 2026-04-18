from dataclasses import dataclass

# Strongest separators first. If none of these break a piece below chunk_size,
# we hard-wrap at character boundaries in _separate's final branch.
SEPARATORS = ["\n\n", "\n", ". ", ", ", " "]


@dataclass(frozen=True)
class ChunkData:
    """A chunk with its overlap context. The embedding is computed from
    `overlapped()` so the vector 'knows' about its neighbors, but only
    `clean` is stored in the database — no text duplication between rows."""
    clean: str
    head: str
    tail: str

    def overlapped(self) -> str:
        return self.head + self.clean + self.tail


def chunk(text: str, chunk_size: int, overlap: float) -> list[ChunkData]:
    """Recursive character text splitter: split by separator hierarchy,
    then greedily merge adjacent pieces up to chunk_size."""
    if not text:
        return []

    clean = _merge(_separate(text.strip(), chunk_size), chunk_size)
    overlap_chars = int(chunk_size * overlap)
    result: list[ChunkData] = []

    for i, piece in enumerate(clean):
        head = clean[i - 1][-overlap_chars:] if (overlap_chars > 0 and i > 0) else ""
        tail = clean[i + 1][:overlap_chars] if (overlap_chars > 0 and i < len(clean) - 1) else ""
        result.append(ChunkData(clean=piece, head=head, tail=tail))

    return result


def _separate(text: str, chunk_size: int, depth: int = 0) -> list[str]:
    if depth >= len(SEPARATORS):
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    sep = SEPARATORS[depth]
    parts = text.split(sep)
    result: list[str] = []

    for i, part in enumerate(parts):
        if len(part) < chunk_size:
            # Reattach the separator as a suffix so _merge can reconstruct
            # the original text without losing whitespace or newlines.
            suffix = sep if i < len(parts) - 1 else ""
            result.append(part + suffix)
        else:
            result.extend(_separate(part, chunk_size, depth + 1))

    return result


def _merge(splits: list[str], chunk_size: int) -> list[str]:
    raw: list[str] = []
    for s in splits:
        if not s:
            continue

        if raw and len(raw[-1]) + len(s) < chunk_size:
            raw[-1] += s
        else:
            raw.append(s)

    return raw
