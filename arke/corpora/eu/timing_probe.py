"""Time individual CELLAR fetches to identify slow tail."""
import json
import time
from pathlib import Path
import requests
from arke.corpora._paths import DATA, ENV_FILE


UA = "arke-research/0.1 (timing-probe; ivankrylov684@gmail.com)"
WORKS = DATA / "eu_pool/works.jsonl"

works = []
with WORKS.open() as f:
    for line in f:
        works.append(json.loads(line))

# Take 20 random
import random

random.seed(42)
sample = random.sample(works, 20)

sess = requests.Session()
times = []
for i, w in enumerate(sample):
    uuid = w["cellar_uuid"]
    if not uuid: continue
    uri = f"http://publications.europa.eu/resource/cellar/{uuid}"
    t0 = time.time()
    try:
        r = sess.get(uri, headers={"User-Agent": UA, "Accept": "application/xml;notice=tree"},
                     timeout=60, allow_redirects=True)
        dt = time.time() - t0
        size = len(r.content)
        print(f"  {i:>2}: {dt:>6.2f}s  status={r.status_code}  size={size:>9}b  celex={w.get('celex','?')}")
        times.append(dt)
    except Exception as e:
        dt = time.time() - t0
        print(f"  {i:>2}: {dt:>6.2f}s  ERR {type(e).__name__}: {e}  celex={w.get('celex','?')}")
    time.sleep(0.2)

if times:
    print(f"\nstats: n={len(times)} min={min(times):.2f} max={max(times):.2f} avg={sum(times)/len(times):.2f}")
