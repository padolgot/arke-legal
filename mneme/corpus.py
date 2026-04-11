"""Built-in corpus for eval. Downloads SQuAD so users can run eval without their own data."""
import json
from urllib.request import urlopen

SQUAD_URL = "https://rajpurkar.github.io/SQuAD-explorer/dataset/dev-v2.0.json"
SQUAD_LIMIT = 200


def download_squad() -> list[dict]:
    print("downloading SQuAD dev set...")
    raw = json.loads(urlopen(SQUAD_URL).read())

    docs = []
    for article in raw["data"]:
        title = article["title"]
        for para in article["paragraphs"]:
            context = para["context"].strip()
            if len(context) < 50:
                continue
            docs.append({
                "content": context,
                "source": title,
                "metadata": {"dataset": "squad-v2"},
            })
            if len(docs) >= SQUAD_LIMIT:
                break
        if len(docs) >= SQUAD_LIMIT:
            break

    print(f"downloaded {len(docs)} paragraphs")
    return docs
