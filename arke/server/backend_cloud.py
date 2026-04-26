"""Cloud backend — OpenAI API via HTTP."""
import json
import logging
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

EMBED_BATCH_SIZE = 2048
RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY = 2.0

logger = logging.getLogger(__name__)


@dataclass
class CloudEmbedder:
    base_url: str
    api_key: str
    model: str

    def embed(self, texts: list[str]) -> list[list[float]]:
        result: list[list[float]] = []
        for offset in range(0, len(texts), EMBED_BATCH_SIZE):
            batch = texts[offset : offset + EMBED_BATCH_SIZE]
            result.extend(self._embed_batch(batch))
        return result

    def _embed_batch(self, batch: list[str]) -> list[list[float]]:
        body = {"model": self.model, "input": batch}
        try:
            res = _post(self.base_url, self.api_key, "/v1/embeddings", body)
        except urllib.error.HTTPError as e:
            if e.code in (400, 413) and len(batch) > 1:
                mid = len(batch) // 2
                logger.warning("embed batch too large (HTTP %d, size %d) — halving", e.code, len(batch))
                return self._embed_batch(batch[:mid]) + self._embed_batch(batch[mid:])
            raise
        data = sorted(res["data"], key=lambda d: d["index"])
        return [d["embedding"] for d in data]


@dataclass
class CloudLLM:
    base_url: str
    api_key: str
    model: str

    def chat(self, system: str | None, user: str) -> str:
        messages: list[dict] = []
        if system is not None:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": user})
        res = _post(self.base_url, self.api_key, "/v1/chat/completions", {"model": self.model, "messages": messages})
        return res["choices"][0]["message"]["content"]


def load(base_url: str, api_key: str, embed_model: str, inference_model: str) -> tuple[CloudEmbedder, CloudLLM]:
    return (
        CloudEmbedder(base_url, api_key, embed_model),
        CloudLLM(base_url, api_key, inference_model),
    )


def _post(base_url: str, api_key: str, path: str, body: dict) -> dict:
    req = urllib.request.Request(
        base_url + path,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    delay = RETRY_BASE_DELAY
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            retriable = e.code == 429 or 500 <= e.code < 600
            last_attempt = attempt == RETRY_ATTEMPTS - 1
            if not retriable or last_attempt:
                raise
            retry_after = e.headers.get("Retry-After") if e.headers else None
            base = float(retry_after) if retry_after and retry_after.replace(".", "").isdigit() else delay
            # Jitter prevents thundering-herd retries from concurrent workers.
            wait = base + random.uniform(0, 1.5)
            logger.warning("cloud %s %d — retry in %.1fs (attempt %d/%d)", path, e.code, wait, attempt + 1, RETRY_ATTEMPTS)
            time.sleep(wait)
            delay *= 2
