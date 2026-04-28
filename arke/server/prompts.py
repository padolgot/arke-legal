"""All LLM prompts for Arke pipelines, in one place for review.

Every system prompt that drives a stage of the stress-test pipeline (or the
walk-mode ingest case-name extractor) lives here. Edit, restart server, done.

Conventions:
- The pipeline LLMs are sculptors. They drop, strip, curate. They do not
  narrate, hedge, or speak in their own voice — output is JSON or trimmed
  verbatim, never AI-assistant prose.
- Role-framing ("you are senior counsel", "act as opposing counsel") is
  reserved for prompts where it materially helps recognition. For pure
  curation/trimming we describe the operation, not the identity.
"""

# Stage 4 — per-doc adversarial chunk selection (cheap LLM, parallel across
# top-N candidate documents). Returns chunk indices to keep.
PER_DOC_PROMPT = (
    "You read one document. Identify ONLY the chunks whose operative legal "
    "substance directly contradicts, weakens, or limits the specific "
    "propositions the lawyer's argument depends on. Treat each chunk as a "
    "potential weapon for opposing counsel — would a barrister actually "
    "point to this exact passage on the bench to defeat the argument?\n"
    "\n"
    "Return ONLY a JSON array of chunk indices, e.g. [3, 14].\n"
    "\n"
    "Hostility test (apply to every chunk):\n"
    "- The chunk's ratio or holding directly conflicts with one of the "
    "argument's load-bearing assumptions: YES → include.\n"
    "- The chunk states general doctrine that aligns with the argument, or "
    "is silent on the specific propositions: NO → exclude.\n"
    "- The chunk is procedural narrative, factual recital, or background: "
    "NO → exclude.\n"
    "- Borderline / unsure: NO → exclude. Default no.\n"
    "\n"
    "Calibration: most chunks in a judgment will NOT qualify, even in a "
    "document that overall cuts against the argument. The hostile core of "
    "any single judgment is small. If you find yourself selecting more than "
    "a small minority of the document's chunks, you are over-selecting — "
    "re-apply the hostility test more strictly.\n"
    "\n"
    "- Return [] if nothing in the document directly attacks the argument.\n"
    "- No explanation. JSON array only."
)

# Stage 5 — strong LLM curates the final mosaic across all surviving docs.
# Sees the big picture; drops noisy/duplicate passages; carves an ensemble of
# labels. Single-source preserved structurally by the per-doc JSON schema.
MOSAIC_SYSTEM_PROMPT = (
    "You curate the final adversarial mosaic the lawyer will rely on. The "
    "input is a JSON object: keys are document identifiers, values are "
    "candidate adversarial passages from that document, already pre-filtered "
    "as broadly hostile. You see all documents at once.\n"
    "\n"
    "For each document, KEEP the one or two passages whose ratio or holding "
    "lands hardest against the lawyer's argument, and write a label — a "
    "tight legal proposition (single phrase, doctrinal, specific to the "
    "argument, not a case name) that THIS document establishes against the "
    "position. The label must be supported by the kept passages.\n"
    "\n"
    "Each surviving document keeps its own passages — never merge across "
    "documents (the schema forbids it).\n"
    "\n"
    "Where two documents establish substantially the same proposition, keep "
    "the one whose passages state it most directly; drop the weaker echo.\n"
    "\n"
    "The labels across surviving documents form an ENSEMBLE — read them in "
    "sequence and they should sound like strikes in the same key, "
    "complementary in force, no two saying the same thing.\n"
    "\n"
    "Output STRICTLY a JSON object — same keys as input, only for docs you "
    "kept:\n"
    '{"<doc_key>": {"keep": [<passage indices to keep>], "label": "<phrase>"}, ...}\n'
    "\n"
    "Rules:\n"
    "- keep: list of 0-indexed passage positions from that document. Take "
    "as few as land cleanly; take more only if each adds independent force.\n"
    "- label: doctrinal proposition AGAINST the lawyer's argument.\n"
    "  Good: 'Duty found where reliance was specific and known'\n"
    "  Bad:  'Caparo precedent' (just names case), 'Duty limits' (too generic)\n"
    "- Default to keeping documents that contain genuinely hostile material. "
    "Output {} only when every input document's passages, on close reading, "
    "do not actually attack this argument — not as a safe-harbour exit.\n"
    "- Output ONLY the JSON object."
)

# Stage 7 — cheap LLM trims procedural narrative inside blockquotes.
# Verbatim-only output, bracket-ellipsis for cut spans, structure preserved.
TRIMMER_SYSTEM_PROMPT = (
    "You receive a mosaic of case-law excerpts chosen as adversarial authority. "
    "Your job is to trim procedural narrative, lead-in, and background from "
    "within each quoted passage, leaving only the operative legal substance — "
    "the ratio, the holding, the doctrinal statement.\n"
    "\n"
    "Rules:\n"
    "- Within each blockquote, DELETE procedural text (recitations of claim "
    "paragraphs, statement-of-claim references, background facts, procedural "
    "history) and REPLACE the deleted span with '[…]' (bracket-ellipsis).\n"
    "- NEVER remove a blockquote entirely.\n"
    "- NEVER rewrite, paraphrase, or add new words. The ONLY new text you "
    "may introduce is '[…]'. Every remaining word must appear verbatim in "
    "the input.\n"
    "- Preserve the structure exactly: ## label headers, > blockquotes, "
    "— source-citation lines (the entire pipe-separated metadata after — "
    "stays verbatim).\n"
    "- If a passage is already lean (mostly ratio / holding), leave it as-is.\n"
    "\n"
    "Output the trimmed markdown. No preamble, no explanation."
)

# Walk-mode ingest case-name extractor (cheap LLM). Bypassed in manifest mode
# where loader.py mirrors manifest['title'] into doc.metadata['case_name'].
# Reachable when ingesting a SharePoint/OneDrive corpus without manifest.jsonl.
CASE_NAME_PROMPT = (
    "Return a one-line label for this document.\n"
    "\n"
    "FIRST decide: is this a court judgment with named parties?\n"
    "\n"
    "IF YES → return ONLY the case title. Nothing else.\n"
    "Format: 'Party A v Party B [Year]' — year in square brackets ONLY if "
    "clearly stated in the document. If year is absent, omit the brackets "
    "entirely — never write the literal '[Year]'.\n"
    "Do NOT prefix with 'Case judgment,', 'Judgment on,', 'Court decision,' "
    "or any descriptor. The case title stands alone.\n"
    "  Caparo Industries v Dickman [1990]\n"
    "  R (Miller) v Prime Minister [2019]\n"
    "  Baird Textile Holdings Ltd v Marks and Spencer plc\n"
    "\n"
    "IF NO (contract, memo, letter, witness statement, expert report, opinion, "
    "email, pleading, research note, etc.) → return a brief descriptor: "
    "document type + subject + date if available.\n"
    "  Engagement letter, Smith Holdings audit, January 2022\n"
    "  Witness statement of James Wilson, March 2024\n"
    "  Expert report on construction defects, Dr Jane Smith, 2020\n"
    "\n"
    "Hard rules:\n"
    "- One line, plain text, no quotes, no trailing punctuation.\n"
    "- Never include the word 'unknown' inside the label — if a party or date "
    "is unknown, omit that piece.\n"
    "- Never include literal placeholders like '[Year]' or '[Date]'.\n"
    "- If the document's nature is genuinely impossible to identify at all, "
    "return exactly the single word: unknown"
)
