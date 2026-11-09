import json
import logging
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from src import config

log = logging.getLogger(__name__)

SPARK_METADATA_PREFIXES = ("_", ".")


def _relative_to_root(path):
    path = Path(path).resolve()
    try:
        return path.relative_to(config.ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _data_files(path):
    if path.is_file():
        return [path]
    return [
        file for file in path.rglob("*")
        if file.is_file()
        and not any(part.startswith(SPARK_METADATA_PREFIXES) for part in file.relative_to(path).parts)
    ]


def describe_path(path, rows=None):
    path = Path(path)
    info = {"path": _relative_to_root(path), "rows": rows}
    if not path.exists():
        return {**info, "exists": False}
    files = _data_files(path)
    return {**info, "files": len(files), "bytes": sum(file.stat().st_size for file in files)}


@dataclass
class StageRecord:
    stage: str
    started_at: str
    status: str = "running"
    duration_s: float = 0.0
    error: str | None = None
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    def input(self, path, rows=None):
        self.inputs.append(self._describe("entrada", path, rows))

    def output(self, path, rows=None):
        self.outputs.append(self._describe("salida", path, rows))

    def _describe(self, kind, path, rows):
        info = describe_path(path, rows)
        log.info(
            "%s %s: %s filas, %s archivos, %s bytes",
            kind, info["path"], info["rows"], info.get("files", 0), info.get("bytes", 0),
        )
        return info


def _write_record(record):
    path = config.EVIDENCE / f"{record.stage}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(record), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


@contextmanager
def stage(name):
    record = StageRecord(stage=name, started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    start = time.perf_counter()
    log.info("etapa %s: inicio", name)
    try:
        yield record
        record.status = "ok"
    except Exception as exc:
        record.status = "failed"
        record.error = f"{type(exc).__name__}: {exc}"
        log.exception("etapa %s: error", name)
        raise
    finally:
        record.duration_s = round(time.perf_counter() - start, 2)
        evidence_file = _write_record(record)
        log.info(
            "etapa %s: %s en %.2fs, evidencia en %s",
            name, record.status, record.duration_s, _relative_to_root(evidence_file),
        )
