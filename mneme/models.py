import httpx

from .config import Config, OLLAMA, OPENAI

EMBED_BATCH_SIZE = 2048


async def embed(cfg: Config, http: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    """Embeds texts in batches to respect API limits."""
    all_vectors: list[list[float]] = []
    for offset in range(0, len(texts), EMBED_BATCH_SIZE):
        batch = texts[offset: offset + EMBED_BATCH_SIZE]
        vectors = await _embed_batch(cfg, http, batch)
        all_vectors.extend(vectors)
    return all_vectors


async def _embed_batch(cfg: Config, http: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    if cfg.provider == OPENAI:
        url = f"{cfg.embedder_url}/v1/embeddings"
        body = {"model": cfg.embedder_model, "input": texts}
        res = await _post(http, url, cfg.provider_api_key, body)
        data = sorted(res["data"], key=lambda d: d["index"])
        return [d["embedding"] for d in data]
    elif cfg.provider == OLLAMA:
        url = f"{cfg.embedder_url}/api/embed"
        body = {"model": cfg.embedder_model, "input": texts}
        res = await _post(http, url, "", body)
        return res["embeddings"]
    else:
        raise ValueError(f"unknown provider: {cfg.provider}")


async def chat(cfg: Config, http: httpx.AsyncClient, system: str | None, user: str) -> str:
    messages: list[dict[str, str]] = []
    if system is not None:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})

    if cfg.provider == OPENAI:
        url = f"{cfg.inference_url}/v1/chat/completions"
        body = {"model": cfg.inference_model, "messages": messages}
        res = await _post(http, url, cfg.provider_api_key, body)
        return res["choices"][0]["message"]["content"]
    elif cfg.provider == OLLAMA:
        url = f"{cfg.inference_url}/api/chat"
        body = {"model": cfg.inference_model, "stream": False, "messages": messages}
        res = await _post(http, url, "", body)
        return res["message"]["content"]
    else:
        raise ValueError(f"unknown provider: {cfg.provider}")


async def _post(http: httpx.AsyncClient, url: str, api_key: str, body: dict) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    res = await http.post(url, headers=headers, json=body, timeout=60.0)
    if res.status_code >= 400:
        raise RuntimeError(f"API {res.status_code}: {res.text}")
    return res.json()
