# corpora — UK CAT + EU CELLAR corpus pipeline

Two-source pipeline that produces the unified retrieval corpus for Arke.
This package is **not part of the Arke runtime** — it's a one-time/periodic
batch job. Output lives at `<umbrella>/corpora/` (sibling of the engine repo)
and is read by Arke at retrieval time.

```
corpora/
├── README.md       ← you are here
├── cat/            ← UK Competition Appeal Tribunal corpus (993 docs)
│   └── NOTES.md    ← script-level detail for the cat side
└── eu/             ← EU CJEU + General Court canon (2,383 docs)
```

---

## Final corpus

```
<umbrella>/corpora/
├── cat_raw/                        ← raw CAT scrape (6,775 PDFs, 1.9GB)
├── cat_skeleton/                   ← filtered + EU-citation-extracted
│   ├── pdfs/, text/                  993 skeleton-grade PDFs + plain text
│   ├── manifest_skeleton.jsonl       row per UK doc (sha1, parties, …)
│   └── eu_clusters_phaseB.jsonl      1,111 dedup'd EU citations from CAT
├── eu_pool/                        ← raw EU CELLAR scrape (2,383 docs)
│   ├── raw/                          html/xhtml/pdf as fetched
│   ├── text/                         plain text per cellar_uuid
│   ├── works.jsonl                   2,104 in-scope works (SPARQL output)
│   ├── works_with_manif.jsonl        + best English manifestation URI
│   ├── manifest.jsonl                row per fetched work (celex, ecli, subject_matters, …)
│   └── manifest_text.jsonl           manifest with text_path appended
├── comp_corpus/                      ← FULL mount for Arke (production demo)
│   ├── manifest.jsonl                3,377 docs full corpus
│   ├── manifest_cited.jsonl          2,311 docs filter (subset of above)
│   ├── citation_graph.jsonl          ~30k directed edges
│   ├── citations_raw.jsonl           ~150k raw extracted citations
│   ├── uk/cat/{category}/{year}/{sha1[:16]}.txt
│   └── eu/{court}/{primary_subject}/{year}/{cellar_uuid[:16]}.txt
│
└── comp_corpus_cited/                ← CITED mount for Arke (EVAL/sweep fast iter)
    ├── manifest.jsonl                2,311 docs (cite_in ≥ 1)
    ├── citation_graph.jsonl          ~23k edges, self-contained subset
    ├── uk/cat/{category}/{year}/{sha1[:16]}.txt   (subset symlinks)
    └── eu/{court}/{primary_subject}/{year}/{cellar_uuid[:16]}.txt
```

**Two physical mount points** so Arke can be pointed at either via single
data-root path. `comp_corpus/` for production demo, `comp_corpus_cited/` for
fast EVAL/sweep. In `comp_corpus_cited/` the `cite_in_count` is recomputed
relative to the subset (edges from outside dropped); original full-corpus
counts preserved as `cite_in_count_full`.

**Filenames are HASHED on purpose** — robust against collisions and special
chars in case names. All human metadata (party_slug, canonical_id, citation
forms) lives in `manifest.jsonl`. Read manifest, never parse filename.

---

## Pipeline overview

### UK side — `cat/`

CAT (Competition Appeal Tribunal) Document Archive at `catribunal.org.uk`.
The trib publishes openly, robots-friendly, no anti-bot.

```
catribunal.org.uk → scraper.py → cat_raw/{pdfs,manifest.jsonl}
                  → extract.py (pypdfium2)  → cat_raw/text/
                  → filter_skeleton.py → cat_skeleton/ (993 docs)
                  → extract_eu_citations.py (15-pattern regex)
                  → dedup_phase_a.py (mechanical) → 1,774 clusters
                  → dedup_phase_b.py + retry + pass3 (LLM) → 1,111 clusters
```

The 1,111 deduped EU citations served two roles:
1. Validation of the EU scope (stress-test contrast)
2. Whitelist for direct CELEX-fetch of cases CELLAR's subject-matter
   tagging missed (e.g. C-67/13 Cartes Bancaires tagged only `Competition`
   broad, not the narrow ENTR/POSI/PRAT/EXCL we filtered on)

### EU side — `eu/`

CJEU + General Court case-law via EUR-Lex CELLAR (the Publications Office's
RDF triplestore). NOT via the EUR-Lex frontend (that's behind CloudFront
WAF and returns 202/0 to non-browser clients).

```
CELLAR SPARQL → list_works.py             → works.jsonl (2,104 works)
              → list_manifestations.py    → works_with_manif.jsonl
              → download.py (8 parallel)  → raw/{cellar_uuid}.{fmt}
              → retry_failed.py           → +rescued via alt format
              → playwright_fallback.py    → +rescued via EUR-Lex frontend
              → whitelist_fetch.py        → +rescued via direct CELEX fetch
              → extract_text.py           → text/{cellar_uuid}.txt
```

### Unification + graph

Both sides flow into comp_corpus/ via the eu/ folder's scripts:

```
build_corpus.py     → flatten symlinks to comp_corpus/{uk,eu}/.../{hash}.txt + manifest.jsonl
extract_citations.py   → scan all 3,377 texts with combined regex → citations_raw.jsonl
build_graph.py         → resolve citations to doc_ids → citation_graph.jsonl + cite_in/out per doc
split_versions.py      → cited subset (cite_in ≥ 1) for fast EVAL
```

---

## Sources — what worked, what didn't

### EUR-Lex frontend (`eur-lex.europa.eu/legal-content/...`)

**Don't use directly.** Behind CloudFront WAF — header `x-amzn-waf-action: challenge`.
Returns HTTP 202 with empty body to curl/requests. Looks like async render
or rate-limit but is actually a JS-challenge that requires browser execution.

Used as **last-resort fallback** via Playwright (`playwright_fallback.py`):
headless Chromium passes the JS challenge, returns full HTML. ~7s per page.
Recovered 173 works no other channel could fetch.

### CELLAR SPARQL (`publications.europa.eu/webapi/rdf/sparql`)

**Primary source.** Public OpenLink Virtuoso 8 endpoint, no auth, no WAF.
Permissive, fast (~3 min for 2,000 works metadata pull). POST queries (not
GET) to avoid URL-length limits with large `VALUES` clauses.

Key properties we use:
- `cdm:work_has_resource-type` (JUDG, OPIN_AG, ORDER, ...)
- `cdm:resource_legal_is_about_subject-matter` (CONC, ENTR, POSI, PRAT, EXCL, AIDE, MERG, MARC, ...)
- `owl:sameAs` to celex/ecli URIs (CELEX/ECLI not stored as direct properties)
- `cdm:expression_belongs_to_work` + `cdm:expression_uses_language` for English filtering
- `cdm:manifestation_manifests_expression` + `cdm:manifestation_type` for fetchable file URIs

### CELLAR REST (content delivery)

Each work has manifestations per (language × format). Fetch URL pattern:
`http://publications.europa.eu/resource/cellar/{uuid}.{expr_NNNN}.{manif_NN}/DOC_1`

**Critical: `/DOC_1` suffix gets the actual file content; without it returns
RDF metadata describing the manifestation.** Lost a download cycle to this.

Format priority for English manifestations: `html > xhtml > pdf > pdfa1a > fmx4`.
**Note: `txt` manifestations are LISTED in CELLAR metadata but have no
datastream** — phantom format. Don't prioritize. Causes "datastream not
present" 404 if attempted.

### CURIA (`curia.europa.eu`)

**Don't use.** Mirror of CELLAR content but JSF-rendered (all routing via JS).
robots.txt is permissive, but practical scraping requires Playwright.
CELLAR is the same content via clean API.

### BAILII (`bailii.org`)

**Don't use.** Cloudflare anti-bot challenge ("Making sure you're not a bot")
on every request. Even browser-UA curl gets 4.5KB challenge HTML.

---

## Scope — what we fetched and why

The artery for UK CAT competition law:
**Chapter II s.18 (Art 102 abuse of dominance) + Chapter I s.2 (Art 101 cartels)**.

### Subject-matter scope (CELLAR taxonomy)

```
INCLUDE: tagged Competition (CONC)   — the broad competition bucket
EXCLUDE: tagged AIDE  — State aid (different forum, government-vs-undertaking)
EXCLUDE: tagged MERG  — Concentrations / mergers (ex-ante regulatory)
EXCLUDE: tagged MARC  — Public procurement (different forum)
```

Result: **2,104 works** (1,532 JUDG + 572 OPIN_AG).

**Why broad CONC, not the narrower ENTR/POSI/PRAT/EXCL we used initially:**
CELLAR's own tagging is inconsistent. Cartes Bancaires (C-67/13) — a
foundational Art 101 case cited 356× in our final graph — is tagged only
with broad `Competition`, not with narrow `Agreements/concerted practices`.
Narrow scope misses ~300 such cases. Broad-minus-exclusions is correct.

### Whitelist supplement

For 333 EU citations from CAT skeletons that fell outside our subject-matter
scope (joined cases under different lead CELEX, or CELLAR-tagging quirks),
fetched directly by CELEX/ECLI URI — recovered 296 of them.

### What we deliberately skipped

- AT/COMP Commission decisions (Trucks cartel AT.39824, etc.) — separate
  channel via DG-COMP, not CELLAR JUDG.
- Pre-2014 CAT cases without English-only manifestations (rare).
- Civil Service Tribunal F-prefix cases (staff matters, not competition).
- Horizontal-principle cases (Fedesa proportionality, Marleasing, Upjohn) —
  these get cited 1× per skeleton for one proposition, doctrine well-known,
  no need for full text.

---

## Coverage validation

**Recall (against UK CAT skeletons' actual EU citations):**
- 96.0% of judgment-mentions captured (3,907 of 4,070)
- 88.3% of judgment-keyed clusters in pool

**Precision (regex extraction quality):**
- 99.99% real cites per heuristic classification (only typo `[1194] ECR`
  caught as noise in 150k extractions)
- ~98% recall vs gpt-4o spot-check on 5 random UK docs

**Alpha layer:** 1,510 EU docs (63% of pool) never cited in CAT — these are
doctrinally close but unused. Material for Arke to suggest as new arguments.

---

## How to refresh the corpus

If CAT or CELLAR have new judgments and you want to update:

```bash
# UK side — re-scrape (resumable, will skip already-fetched)
cd cat/
python scraper.py            # incremental (progress.json checkpoint)
python extract.py            # pypdfium2 PDF→text
python filter_skeleton.py    # apply category filter
# only if reverse-extraction needs refresh:
python extract_eu_citations.py
python dedup_phase_a.py
python dedup_phase_b.py      # uses OPENAI key from umbrella .env

# EU side — re-pull SPARQL list, fetch new
cd ../eu/
python list_works.py            # full re-pull (cheap, ~3 min)
python list_manifestations.py   # ~5 min
python download.py              # 8 workers, skips done UUIDs
python retry_failed.py          # rescue alt formats
python playwright_fallback.py   # rescue via frontend (needs playwright)
python whitelist_fetch.py       # rescue CAT-cited misses
python extract_text.py          # html/xhtml/pdf → text

# Unification + graph
python build_corpus.py
python extract_citations.py
python build_graph.py
python split_versions.py        # subset filter manifest (in comp_corpus/)
python make_cited_mount.py      # physical comp_corpus_cited/ mount
```

All scripts are idempotent and resumable (atomic writes, progress.json).

---

## Lessons learned (gotchas list)

1. **CloudFront WAF on EUR-Lex frontend** is silent: returns 202 (Accepted)
   with 0-byte body, looks like async render. Don't waste time retrying.
   Use CELLAR SPARQL or Playwright instead.

2. **CELLAR manifestation `/DOC_1` suffix** is mandatory for content.
   Without it you get RDF metadata describing the manifestation. Cost
   one full download cycle when missed.

3. **CELLAR txt manifestations are phantom** — listed in metadata, no
   datastream. Always returns 404. Use html/xhtml/pdf priority instead.

4. **CELEX format**: post-1989 ECJ uses double-letter `CJ` (e.g. `61986CJ0062`),
   not single `J` as some old guides claim. Pre-1989 also uses `CJ`. Same
   for `TJ` General Court. `J`-only variants do not exist as resources —
   404. We learned this empirically.

5. **Joined cases**: CJEU joins multiple cases with same judgment, registered
   in CELLAR under ONE lead CELEX. CAT skeletons cite by ANY participant
   case number → graph-resolution misses unless you add ECLI alias lookup.
   ~85 of the original 111 unresolved clusters were this artifact.

6. **CELLAR taxonomy is inconsistent**: many works tagged only with broad
   `Competition` (CONC) without the narrow Art 101/102 subdomains. Filter
   by CONC \\ exclusions instead of narrow subdomains alone.

7. **OpenAI rate limits** matter for Phase B dedup: ~30-90k TPM, calls of
   ~21k tokens succeed in isolation but fail back-to-back without sleep.
   Use chunks=100 + sleep=60s for stability.

8. **CAT case ref format** is `<num>/<section>/<court>/<2-digit-year>` —
   case_num can be 1-5 digits with optional `-N` sub-case suffix.
   `1517/11/7/22` means case 1517, NOT year 1517. Old regex assumed
   year-first; cost ~5% of UK citation recall before fixed.

9. **Filenames must be hash, not slug.** Slug-based filenames hit collisions,
   path traversal (`/` in case_ref → nested folders), truncation, and i18n
   issues. Hash filename + manifest with all human metadata is robust.

10. **gpt-4o hallucinates citation placeholders** when document has few
    real citations — echoes prompt examples back as if they were cites
    (`[YYYY] N CMLR N`, `Case N/YY`). Filter LLM-extracted cites against
    placeholder shapes when validating recall.

---

## Output schema (manifest.jsonl)

```jsonc
// UK CAT entry
{
  "doc_id": "uk_73869b0569eb",
  "source": "uk_cat",
  "corpus_path": "uk/cat/judgment-or-ruling/2026/73869b0569eb8aeb.txt",
  "canonical_id": "[2026] CAT 36",
  "neutral_citation": "[2026] CAT 36",
  "case_ref": "1766/4/12/26",
  "party_slug": "Aramark-v-CMA",
  "title": "Aramark Limited v Competition and Markets Authority",
  "date": "2026-04-23",
  "doc_type": "Ruling (Consequential matters)",
  "category": "B_judgment_or_ruling",
  "url": "https://www.catribunal.org.uk/...",
  "text_chars": 37028,
  "page_count": 19,
  "sha1": "73869b0569eb8aeb3655690d666e8aef057cc308",
  "cite_in_count": 0,
  "cite_out_count": 1
}

// EU CELLAR entry
{
  "doc_id": "eu_4073c8b0",
  "source": "eu_cellar",
  "corpus_path": "eu/ecj/dominance/1976/4073c8b0-f562-48.txt",
  "canonical_id": "85-76",
  "celex": "61976CJ0085",
  "ecli": "ECLI:EU:C:1979:36",
  "case_num": "85-76",
  "party_slug": "Hoffmann-La-Roche-v-Commission",
  "title": "Judgment of the Court of 13 February 1979.#Hoffmann-La Roche...",
  "date": "1979-02-13",
  "doc_type": "JUDG",
  "subject_matters": ["Agreements...","Dominant position","Exclusive agreements"],
  "primary_subject": "dominance",
  "court": "ecj",
  "year": "1976",
  "text_chars": 115931,
  "format_origin": "html",
  "via": "cellar_rest",
  "cellar_uuid": "4073c8b0-f562-488f-a435-768f4efae3b2",
  "cite_in_count": 331,
  "cite_out_count": 0
}
```

`citation_graph.jsonl` rows: `{source_doc_id, target_doc_id, count}`.

---

## Per-folder scripts

### `cat/` — UK CAT scraper
See `cat/NOTES.md` for script-level detail. Canonical pipeline:
`scraper.py → extract.py → filter_skeleton.py → extract_eu_citations.py →
dedup_phase_a.py → dedup_phase_b.py (+ retry + pass3)`. Plus 4 alien-regex
experiments kept as historical record of how the 15-pattern frozen regex
was developed.

### `eu/` — EU CELLAR scraper
Pipeline scripts (canonical execution order):

| Script | Role |
|--------|------|
| `list_works.py` | SPARQL pull all in-scope works (CONC \\ AIDE \\ MERG \\ MARC) |
| `list_manifestations.py` | Bulk SPARQL: best English manifestation URI per work |
| `download.py` | Parallel fetch via cellar manifestation URI + `/DOC_1` |
| `retry_failed.py` | Try ALL English manifestations for failures |
| `playwright_fallback.py` | EUR-Lex frontend via headless Chromium for stragglers |
| `whitelist_fetch.py` | Direct CELEX/ECLI fetch for CAT-cited cases outside subject scope |
| `extract_text.py` | html/xhtml/pdf → plain text (pypdfium2 for PDFs) |
| `build_corpus.py` | Flatten UK + EU into hash-named symlink tree + unified manifest |
| `extract_citations.py` | Regex extraction across full corpus (15 EU + 22 UK patterns) |
| `build_graph.py` | Resolve citations to doc_ids, compute cite_in/out, write graph |
| `split_versions.py` | Build cited subset manifest filter (`comp_corpus/manifest_cited.jsonl`) |
| `make_cited_mount.py` | Build physical `comp_corpus_cited/` mount with own symlinks + filtered graph |

Probe / audit / experimental scripts (kept as record of journey):

| Script | Purpose |
|--------|---------|
| `probe.py`, `probe2.py`, `probe3.py`, `probe4.py` | EUR-Lex / CURIA / BAILII reachability discovery |
| `sparql_probe.py`, `sparql_probe2.py`, `sparql_probe3.py` | CELLAR SPARQL property/structure discovery |
| `format_probe.py`, `fetch_probe.py`, `fetch_probe2.py` | Manifestation format coverage |
| `manifestation_probe.py` | Full manifestation listing per work |
| `timing_probe.py` | Per-request CELLAR latency |
| `count_artery.py` | Pre-fetch scope size validation |
| `stresstest_scope.py` | Early keyword classifier (28% recall — abandoned for CELLAR ground truth) |
| `stresstest_via_cellar.py` | CELLAR ground-truth contrast (final approach) |
| `regex_audit.py` | Coverage check vs broad candidate patterns |
| `precision_check.py` | False-positive rate via heuristic classification |
| `spot_check.py` | Recall validation vs gpt-4o LLM extraction |
| `contrast.py` | Final reverse-extraction vs scraped pool stress-test |
