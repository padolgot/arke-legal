"""Local backend — llama-cpp-python, models loaded in-process from .gguf files."""
from dataclasses import dataclass

EMBED_BATCH_SIZE = 64


@dataclass
class LocalEmbedder:
    _model: object

    def embed(self, texts: list[str]) -> list[list[float]]:
        result: list[list[float]] = []
        for offset in range(0, len(texts), EMBED_BATCH_SIZE):
            batch = texts[offset : offset + EMBED_BATCH_SIZE]
            result.extend(self._model.embed(batch))
        return result


@dataclass
class LocalLLM:
    _model: object

    def chat(self, system: str | None, user: str) -> str:
        messages: list[dict] = []
        if system is not None:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": user})
        response = self._model.create_chat_completion(messages=messages)
        return response["choices"][0]["message"]["content"]


def load(embed_model_path: str, inference_model_path: str) -> tuple[LocalEmbedder, LocalLLM]:
    from llama_cpp import Llama
    embedder = Llama(model_path=embed_model_path, embedding=True, n_ctx=512, verbose=False)
    llm = Llama(model_path=inference_model_path, n_ctx=4096, verbose=False)
    return LocalEmbedder(embedder), LocalLLM(llm)
