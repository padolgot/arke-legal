import hashlib
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class RcloneSource:
    def __init__(self, name: str, remote: str):
        self._name = name
        self._remote = remote

    @property
    def name(self) -> str:
        return self._name

    def sync_to(self, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["rclone", "sync", self._remote, str(dest),
             "--timeout", "10m",
             "--contimeout", "60s"],
        )
        # exit 3 = rclone "directory not found" — source was deleted.
        # Treat as empty: drop the local copy so stale data can't linger.
        if result.returncode == 3:
            shutil.rmtree(dest, ignore_errors=True)
            return
        result.check_returncode()


def _purge_orphans(staging: Path, active: set[str]) -> None:
    for child in staging.iterdir():
        if child.is_dir() and child.name not in active:
            logger.info("purging orphan source: %s", child.name)
            shutil.rmtree(child)


def _dir_hash(path: Path) -> str:
    h = hashlib.md5()
    for f in sorted(path.rglob("*")):
        if f.is_file():
            st = f.stat()
            h.update(str(f.relative_to(path)).encode())
            h.update(str(st.st_size).encode())
            h.update(str(st.st_mtime_ns).encode())
    return h.hexdigest()


def _load_hash(space: Path) -> str:
    p = space / ".sync_hash"
    return p.read_text().strip() if p.exists() else ""


def _save_hash(space: Path, h: str) -> None:
    (space / ".sync_hash").write_text(h)


def run(space: Path, sources: list[RcloneSource], interval: int = 60) -> None:
    """Sync loop. Blocks forever — call from a dedicated process.

    TODO robustness (ranked):
      1. partial rclone sync → staging holds garbage, next hash publishes broken
         digest. Per-source .synced marker or tmpdir-per-source + atomic swap.
      2. disk full during staging → digest.tmp copytree → broken digest gets
         published anyway. Check shutil.disk_usage before copytree.
      3. mtime-based _dir_hash is fragile under clock skew / touch without
         change. Content hash is accurate but costs O(corpus) per tick.
    """
    staging = space / "staging"
    digest = space / "digest"
    staging.mkdir(parents=True, exist_ok=True)

    # survive restarts without forcing a full re-ingest
    last_hash = _load_hash(space)

    while True:
        for source in sources:
            try:
                source.sync_to(staging / source.name)
            except Exception as exc:
                logger.warning("source %s failed: %s", source.name, exc)

        _purge_orphans(staging, {s.name for s in sources})

        current_hash = _dir_hash(staging)
        if current_hash != last_hash:
            tmp = digest.with_name(digest.name + ".tmp")
            old = digest.with_name(digest.name + ".old")

            if tmp.exists():
                shutil.rmtree(tmp)
            if old.exists():
                shutil.rmtree(old)

            shutil.copytree(staging, tmp, symlinks=True)

            if digest.exists():
                # This rename is the only junction between the sync daemon and Arke.
                # If Arke consumed digest between exists() and here, this raises — let it.
                # systemd restarts in 5s, digest is gone, second attempt goes through clean.
                digest.rename(old)

            tmp.rename(digest)

            if old.exists():
                shutil.rmtree(old)

            last_hash = current_hash
            _save_hash(space, last_hash)
            logger.info("digest published")

        time.sleep(interval)


def _parse_sources(raw: str) -> list[RcloneSource]:
    """Parse ARKE_SOURCES. Format: 'name:remote,name2:remote2'.

    'remote' is whatever rclone accepts — a local path (/srv/data) or a
    configured rclone remote (gdrive:bucket/foo). We split on the first ':'
    only, so remotes containing ':' survive intact.
    """
    sources: list[RcloneSource] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        raw_name, sep, raw_remote = entry.partition(":")
        name = raw_name.strip()
        remote = raw_remote.strip()
        if not sep or not name or not remote:
            raise ValueError(f"bad source entry: {entry!r} (want 'name:remote')")
        sources.append(RcloneSource(name, remote))
    return sources


def main() -> None:
    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from arke.server.workspace import path_for

    workspace_name = os.environ.get("ARKE_WORKSPACE", "default")
    workspace_path = path_for(workspace_name)
    workspace_path.mkdir(parents=True, exist_ok=True)

    raw_sources = os.environ.get("ARKE_SOURCES", "").strip()
    if not raw_sources:
        raise SystemExit("ARKE_SOURCES is required (e.g. 'bailii:/srv/bailii,matters:gdrive:matters')")
    sources = _parse_sources(raw_sources)

    interval = int(os.environ.get("ARKE_SYNC_INTERVAL", "60"))
    logger.info("sync starting, workspace=%s, sources=%s, interval=%ds",
                workspace_name, [s.name for s in sources], interval)
    run(workspace_path, sources, interval)


if __name__ == "__main__":
    main()
