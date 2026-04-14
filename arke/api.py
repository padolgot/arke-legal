"""REST API for Arke. Start with `arke serve`."""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from . import Arke
from .config import Config


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
    answer = await engine.ask(req.query)
    return {"answer": answer}


@app.post("/ingest")
async def ingest(req: IngestRequest):
    await engine.ingest(req.source)
    return {"status": "ok"}


@app.post("/sweep")
async def sweep(req: SweepRequest):
    rows = await Arke.sweep(cfg, req.level, req.limit)
    return {"rows": [{**vars(row.metrics), **vars(row.cfg)} for row in rows]}
