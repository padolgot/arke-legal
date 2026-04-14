"""LLM API client. Speaks the /v1/ protocol supported by all major backends."""
import asyncio

import httpx

from .config import Config

EMBED_BATCH_SIZE = 64
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2
RETRYABLE_STATUSES = (429, 502, 503, 504)


async def embed(cfg: Config, http: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    """Embeds texts in batches to respect API limits."""
    all_vectors: list[list[float]] = []
    total = len(texts)
    for offset in range(0, total, EMBED_BATCH_SIZE):
        batch = texts[offset: offset + EMBED_BATCH_SIZE]
        vectors = await _embed_batch(cfg, http, batch)
        all_vectors.extend(vectors)
        done = min(offset + EMBED_BATCH_SIZE, total)
        print(f"\r  embed {done}/{total}", end="", flush=True)
    if total > EMBED_BATCH_SIZE:
        print()
    return all_vectors


async def _embed_batch(cfg: Config, http: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    url = f"{cfg.embedder_url}/v1/embeddings"
    body = {"model": cfg.embedder_model, "input": texts}
    res = await _post(http, url, cfg.api_key, body)
    try:
        data = sorted(res["data"], key=lambda d: d["index"])
        return [d["embedding"] for d in data]
    except (KeyError, TypeError, IndexError) as exc:
        raise RuntimeError(f"unexpected embed response from {url}: {exc}") from exc


async def chat(cfg: Config, http: httpx.AsyncClient, system: str | None, user: str) -> str:
    messages: list[dict[str, str]] = []
    if system is not None:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})

    url = f"{cfg.inference_url}/v1/chat/completions"
    body = {"model": cfg.inference_model, "messages": messages}
    res = await _post(http, url, cfg.api_key, body)
    try:
        return res["choices"][0]["message"]["content"]
    except (KeyError, TypeError, IndexError) as exc:
        raise RuntimeError(f"unexpected chat response from {url}: {exc}") from exc


async def _post(http: httpx.AsyncClient, url: str, api_key: str, body: dict) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    last_err: Exception | None = None

    for attempt in range(RETRY_ATTEMPTS):
        try:
            res = await http.post(url, headers=headers, json=body, timeout=300.0)
        except httpx.TimeoutException as exc:
            last_err = exc
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            raise RuntimeError(f"API timed out after {RETRY_ATTEMPTS} attempts ({url})") from exc
        except httpx.ConnectError as exc:
            last_err = exc
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            raise RuntimeError(f"API unreachable after {RETRY_ATTEMPTS} attempts ({url})") from exc

        if res.status_code in RETRYABLE_STATUSES and attempt < RETRY_ATTEMPTS - 1:
            last_err = RuntimeError(f"API {res.status_code}")
            await asyncio.sleep(RETRY_DELAY)
            continue

        if res.status_code >= 400:
            raise RuntimeError(f"API {res.status_code} from {url}: {res.text[:200]}")

        return res.json()

    raise RuntimeError(f"API failed after {RETRY_ATTEMPTS} attempts ({url}): {last_err}")
