import json
import re

from ..core.models import chat
from .metrics import EvalCase


# Questions must be specific to a single chunk, otherwise eval gives false
# positives from other chunks.
GEN_PROMPT = 'You will receive a text chunk from a personal knowledge base. Generate 1 to 3 questions that ONLY this specific chunk can answer. Questions must include specific details from the text — names, numbers, dates, unique terms. Avoid generic questions. Return ONLY a JSON array of strings, nothing else. Example: ["question 1", "question 2"]\n\nChunk:\n'


async def make_cases(mneme, limit: int) -> list[EvalCase]:
    """Samples random chunks and asks the LLM to invent questions for each.
    Expectation: when searched, each question should return the chunk it
    was generated from — that's the implicit ground truth."""
    samples = await mneme.db.sample(limit)
    cases: list[EvalCase] = []
    for chunk_id, content in samples:
        questions = await _generate_questions(mneme, content)
        for q in questions:
            cases.append(EvalCase(query=q, expected_ids=[chunk_id]))
    return cases


async def _generate_questions(mneme, content: str) -> list[str]:
    raw = (await chat(
        mneme._http, mneme.cfg.inference_url, mneme.cfg.inference_model,
        None, GEN_PROMPT + content,
    )).strip()
    # The model may wrap the array in ```json``` or add commentary —
    # extract the array with a regex.
    match = re.search(r"\[[\s\S]*]", raw)
    if not match:
        return []
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [q for q in parsed if isinstance(q, str) and q]
