"""Cloud backend — OpenAI API via HTTP."""
import json
import urllib.request
from dataclasses import dataclass

EMBED_BATCH_SIZE = 64


@dataclass
class CloudEmbedder:
    base_url: str
    api_key: str
    model: str

    def embed(self, texts: list[str]) -> list[list[float]]:
        result: list[list[float]] = []
        for offset in range(0, len(texts), EMBED_BATCH_SIZE):
            batch = texts[offset : offset + EMBED_BATCH_SIZE]
            res = _post(self.base_url, self.api_key, "/v1/embeddings", {"model": self.model, "input": batch})
            data = sorted(res["data"], key=lambda d: d["index"])
            result.extend(d["embedding"] for d in data)
        return result


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
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())
