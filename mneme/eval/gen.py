import asyncio
import json
import re

from .. import Mneme
from ..core.models import chat
from .metrics import EvalCase

CONCURRENCY = 10


async def make_cases(mneme: Mneme, limit: int) -> list[EvalCase]:
    """Samples random chunks and generates eval questions concurrently."""
    samples = await mneme.db.sample(limit)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    done = 0

    async def process(chunk) -> list[EvalCase]:
        nonlocal done
        async with semaphore:
            questions = await _generate_questions(mneme, chunk.content)
            done += 1
            print(f"  gen {done}/{len(samples)}")
            return [EvalCase(query=q, expected_ids=[chunk.id]) for q in questions]

    tasks = [process(chunk) for chunk in samples]
    results = await asyncio.gather(*tasks)

    cases = [case for batch in results for case in batch]
    print(f"gen done: {len(cases)} cases from {len(samples)} chunks")
    return cases


async def _generate_questions(mneme: Mneme, content: str) -> list[str]:
    instruction = "You will receive a text chunk from a personal knowledge base. Generate 1 to 3 questions that ONLY this specific chunk can answer. Questions must include specific details from the text — names, numbers, dates, unique terms. Avoid generic questions. Return ONLY a JSON array of strings, nothing else. Example: [\"question 1\", \"question 2\"]"
    prompt = instruction + "\n\nChunk:\n" + content
    raw = await chat(mneme.cfg, mneme.http, None, prompt)
    raw = raw.strip()

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
