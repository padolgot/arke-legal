# cat_scraper — script index

Scripts that built the CAT corpus + EU citation pool. Output lives at
`<umbrella>/corpora/cat_raw/` (full scrape) and
`cat_skeleton/` (skeleton-grade subset + EU pool).

See `cat_skeleton/README.md` for the data-side narrative + final state.

## Pipeline scripts (canonical, in execution order)

| Script | Role | Input | Output |
|--------|------|-------|--------|
| `scraper.py` | Scrape CAT Document Archive (all 343 listing pages) | catribunal.org.uk | `cat_raw/pdfs/`, `manifest.jsonl` |
| `smoke_extract.py` | Sanity-check PDF extraction across doc-type categories | `cat_raw/` | stdout report only |
| `extract.py` | PDF → text via pypdfium2, atomic writes | `cat_raw/pdfs/` | `cat_raw/text/`, `manifest_text.jsonl` |
| `filter_skeleton.py` | Keep only authority-grade docs (UKSC/CoA judgments + CAT judgments/rulings); drops admin orders, transcripts, schedules, summaries, consents, transfers | `cat_raw/` | `cat_skeleton/` (993 PDFs / 994 rows) |
| `extract_eu_citations.py` | Frozen 15-pattern regex EU citation extractor (5 standard + R1 winners + R2 winners merged) | `cat_skeleton/text/` | `eu_citations.jsonl` (2,002 entries) |
| `dedup_phase_a.py` | Mechanical dedup by case-num/ECLI/AT/COMP/etc. canonical key. Pure code, zero LLM, zero hallucination risk | `eu_citations.jsonl` | `eu_clusters_phaseA.jsonl` (1,774 clusters) |
| `dedup_phase_b.py` | Phase B pass 1: chunk=200, sleep=10s. Two of four chunks hit 429 — kept anyway, retried below | `eu_clusters_phaseA.jsonl` | `eu_clusters_phaseB.jsonl` |
| `dedup_phase_b_retry.py` | Phase B retry: chunks=100, sleep=60s. Re-processes residual UNKEYED. Recovered all 4 chunks cleanly | `eu_clusters_phaseB.jsonl` | overwrites `eu_clusters_phaseB.jsonl` |
| `dedup_phase_b_pass3.py` | Phase B pass 3: full context-samples per UNKEYED for stubborn residuals. Chunks=60, sleep=60s | `eu_clusters_phaseB.jsonl` | overwrites `eu_clusters_phaseB.jsonl` (final: 1,111 clusters) |

## Experimental scripts (kept as historical record)

| Script | Role |
|--------|------|
| `experiment_alien_regex.py` | Round 1: 8 alien patterns, 4 winners → JOINED, BARE_C, BARE_T, ALT_ECLI, FREE_V_COMM merged into main |
| `experiment_alien_regex_round2.py` | Round 2: Commission AT/COMP/DECISION + AG_OPINION + REGULATION → all 5 merged into main |
| `experiment_alien_regex_round3.py` | Round 3: Directives + CAT-internal precedent + treatises + GC-phrases. Findings noted; not merged (different genre) |
| `experiment_alien_regex_round4.py` | Round 4: TFEU article refs + UK case citations + AG attribution. Findings noted; UK refs covered by skeleton corpus, framework refs out-of-scope |

## Key design choices (recorded for future-me)

- **15 frozen regex patterns** in `extract_eu_citations.py` — that's the canonical extraction. Don't add more without empirical evidence (Round 3+4 hits were already covered or out-of-scope).
- **`FREE_V_COMM` post-filter** drops fragment matches like "Co AG v Commission" / "Others v Commission".
- **Phase A is mechanical** — case-num + ECLI + Commission codes share canonical keys, dedup is deterministic. No LLM needed for those 228 merges.
- **Phase B prompt rule:** LLM only matches an UNKEYED to a keyed cluster when the cluster's canonical key appears explicitly in the UNKEYED's context window. No memory-reliance. Conservative bias preserves zero-hallucination guarantee but leaves 81 standalone UNKEYED that are real cases without inherent identifiers in the text where they were cited.
- **TPM tier ceiling** for the OpenAI key used here is around 30k-90k TPM. Calls of ~21k tokens succeed in isolation but fail on back-to-back without sleep. Phase B retry uses chunks=100 + sleep=60s and is reliable. Don't push above 25k tokens per call without 60+s spacing.
- **Atomic writes** for everything that touches disk (`.tmp + fsync + os.replace`) — per CLAUDE.md robust > clever.
- **Resumable scrapers** (`scraper.py`, `extract.py`) use a `progress.json` checkpoint so a kill-9 doesn't lose state.
