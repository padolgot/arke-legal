from dataclasses import dataclass


DEFAULT_EMBEDDER_URL = "http://localhost:11434"
DEFAULT_EMBEDDER_MODEL = "bge-m3"
DEFAULT_EMBEDDING_DIM = 1024
DEFAULT_INFERENCE_URL = "http://localhost:11434"
DEFAULT_INFERENCE_MODEL = "llama3:8b-instruct-q4_K_M"
DEFAULT_CHUNK_SIZE = 600
DEFAULT_OVERLAP = 0.0
DEFAULT_ALPHA = 0.7
DEFAULT_K = 5


@dataclass(frozen=True)
class MnemeConfig:
    database_url: str
    embedder_url: str = DEFAULT_EMBEDDER_URL
    embedder_model: str = DEFAULT_EMBEDDER_MODEL
    embedding_dim: int = DEFAULT_EMBEDDING_DIM
    inference_url: str = DEFAULT_INFERENCE_URL
    inference_model: str = DEFAULT_INFERENCE_MODEL
    # Chunking: overlap is a fraction of chunk_size, not characters.
    chunk_size: int = DEFAULT_CHUNK_SIZE
    overlap: float = DEFAULT_OVERLAP
    # Hybrid search: alpha=1.0 → pure cosine, 0.0 → pure BM25.
    alpha: float = DEFAULT_ALPHA
    k: int = DEFAULT_K


def resolve_config(cfg: MnemeConfig) -> MnemeConfig:
    """Single validation point. Called from Mneme.__init__."""
    if not cfg.database_url:
        raise ValueError("Mneme: database_url is required")
    if not isinstance(cfg.chunk_size, int) or cfg.chunk_size < 100 or cfg.chunk_size > 10000:
        raise ValueError(f"config: chunk_size must be integer 100..10000, got {cfg.chunk_size}")
    if cfg.overlap < 0 or cfg.overlap > 0.5:
        raise ValueError(f"config: overlap must be 0..0.5 (fraction of chunk_size), got {cfg.overlap}")
    if cfg.alpha < 0 or cfg.alpha > 1:
        raise ValueError(f"config: alpha must be 0..1, got {cfg.alpha}")
    if not isinstance(cfg.k, int) or cfg.k < 1 or cfg.k > 20:
        raise ValueError(f"config: k must be integer 1..20, got {cfg.k}")
    return cfg
