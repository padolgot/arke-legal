"""REST API for Arke. Start with `arke serve`."""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import Arke
from .config import Config

STATIC_DIR = Path(__file__).parent / "static"


engine: Arke
cfg: Config


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global engine, cfg
    cfg = Config.from_env()
    engine = Arke(cfg)
    await engine.open()
    yield
    await engine.close()


app = FastAPI(title="Arke", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    query: str


class IngestRequest(BaseModel):
    source: str


class SweepRequest(BaseModel):
    level: str
    limit: int


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/ask")
async def ask(req: AskRequest):
    result = await engine.ask(req.query)
    sources = [
        {
            "source": h.chunk.source,
            "content": h.chunk.content[:300],
            "similarity": round(h.similarity, 3),
        }
        for h in result.hits
    ]
    return {"answer": result.answer, "sources": sources}


@app.post("/ingest")
async def ingest(req: IngestRequest):
    await engine.ingest(req.source)
    return {"status": "ok"}


@app.post("/sweep")
async def sweep(req: SweepRequest):
    rows = await Arke.sweep(cfg, req.level, req.limit)
    return {"rows": [{**vars(row.metrics), **vars(row.cfg)} for row in rows]}


@app.get("/open/{source:path}")
async def open_file(source: str):
    """Opens a document in the OS default application. Local-only."""
    import subprocess, sys
    base = Path(cfg.data_path)
    full = (base / source).resolve()
    if not full.exists():
        matches = list(base.rglob(source))
        if not matches:
            return {"error": "not found"}
        full = matches[0].resolve()

    if sys.platform == "linux":
        subprocess.Popen(["xdg-open", str(full)])
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(full)])
    else:
        subprocess.Popen(["start", str(full)], shell=True)

    return {"status": "ok"}


if STATIC_DIR.is_dir():
    @app.get("/{path:path}")
    async def spa_fallback(path: str):
        file = STATIC_DIR / path
        if file.is_file():
            return FileResponse(file)
        return FileResponse(STATIC_DIR / "index.html")
