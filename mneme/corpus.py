"""Built-in SQuAD parser. Used by digest when DATA_PATH is a URL."""
import json
from urllib.error import URLError
from urllib.request import urlopen

SQUAD_LIMIT = 200


def download_squad(url: str) -> list[dict]:
    print(f"downloading from {url}...")
    try:
        raw = json.loads(urlopen(url).read())
    except (URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"failed to download {url}: {exc}") from exc

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
