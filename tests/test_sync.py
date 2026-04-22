import subprocess
from pathlib import Path

import pytest

from arke.digest.sync import RcloneSource, _purge_orphans


def test_purge_orphans_removes_inactive_dirs(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "bailii").mkdir()
    (staging / "bailii" / "case.txt").write_text("content")
    (staging / "landmarks").mkdir()
    (staging / "landmarks" / "old.txt").write_text("stale")

    _purge_orphans(staging, {"bailii"})

    assert (staging / "bailii").is_dir()
    assert (staging / "bailii" / "case.txt").read_text() == "content"
    assert not (staging / "landmarks").exists()


def test_purge_orphans_keeps_all_when_all_active(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "bailii").mkdir()
    (staging / "matters").mkdir()

    _purge_orphans(staging, {"bailii", "matters"})

    assert (staging / "bailii").is_dir()
    assert (staging / "matters").is_dir()


def test_purge_orphans_ignores_files(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "bailii").mkdir()
    (staging / ".sync_hash").write_text("abc")

    _purge_orphans(staging, {"bailii"})

    assert (staging / ".sync_hash").exists()


def test_sync_to_drops_local_copy_when_source_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "landmarks"
    dest.mkdir()
    (dest / "stale.txt").write_text("old content")

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, returncode=3)

    monkeypatch.setattr("arke.digest.sync.subprocess.run", fake_run)
    source = RcloneSource("landmarks", "/nonexistent/path")
    source.sync_to(dest)

    assert not dest.exists()


def test_sync_to_raises_on_real_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "bailii"

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, returncode=2)

    monkeypatch.setattr("arke.digest.sync.subprocess.run", fake_run)
    source = RcloneSource("bailii", "/some/path")
    with pytest.raises(subprocess.CalledProcessError):
        source.sync_to(dest)
