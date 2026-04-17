"""Generate eval cases from chunks using LLM.

For each sampled chunk, LLM generates questions that only that chunk can answer.
This creates ground-truth (query → expected_chunk) pairs for scoring.
"""
import json
import random
import re
from dataclasses import dataclass

from arke.server.models import LLM
from arke.server.types import Chunk

INSTRUCTION = (
    "You will receive a text chunk from a legal document. "
    "Generate 1 to 3 questions that ONLY this specific chunk can answer. "
    "Questions must include specific details — names, numbers, dates, case citations, unique terms. "
    "Avoid generic questions. "
    "Return ONLY a JSON array of strings, nothing else. "
    'Example: ["question 1", "question 2"]'
)


@dataclass(frozen=True)
class EvalCase:
    query: str
    expected_key: str  # "doc_id:chunk_index"


def make_cases(llm: LLM, chunks: list[Chunk], limit: int) -> list[EvalCase]:
    samples = random.sample(chunks, min(limit, len(chunks)))
    cases: list[EvalCase] = []

    for i, chunk in enumerate(samples):
        questions = _generate_questions(llm, chunk.clean)
        key = f"{chunk.doc_id}:{chunk.chunk_index}"
        cases.extend(EvalCase(query=q, expected_key=key) for q in questions)
        print(f"  gen {i + 1}/{len(samples)}: {len(questions)} questions")

    print(f"gen done: {len(cases)} cases from {len(samples)} chunks")
    return cases


def _generate_questions(llm: LLM, content: str) -> list[str]:
    raw = llm.chat(None, INSTRUCTION + "\n\nChunk:\n" + content).strip()

    match = re.search(r"\[[\s\S]*]", raw)
    if not match:
        return []

    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []

    if not isinstance(parsed, list):
        return []

    return [q for q in parsed if isinstance(q, str) and q.strip()]
