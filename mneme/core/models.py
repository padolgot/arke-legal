import httpx


async def embed(http: httpx.AsyncClient, url: str, model: str, texts: list[str]) -> list[list[float]]:
    res = await http.post(
        f"{url}/api/embed",
        json={"model": model, "input": texts},
    )
    # No retries: local ollama rarely fails, and when it does the cause is
    # systemic (OOM, model not loaded) — retry won't help.
    if res.status_code >= 400:
        raise RuntimeError(f"Embedder {res.status_code}: {res.text}")
    return res.json()["embeddings"]


async def chat(http: httpx.AsyncClient, url: str, model: str, system: str | None, user: str) -> str:
    messages: list[dict[str, str]] = []
    if system is not None:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})

    res = await http.post(
        f"{url}/api/chat",
        json={"model": model, "stream": False, "messages": messages},
        timeout=120.0,  # cold model load can be slow
    )
    if res.status_code >= 400:
        raise RuntimeError(f"Inference {res.status_code}: {res.text}")
    return res.json()["message"]["content"]
