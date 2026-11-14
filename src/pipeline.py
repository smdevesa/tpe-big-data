import argparse
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version

from src import config
from src.evidence import stage
from src.idempotency import compare, snapshot
from src.logs import configure_logging, log_to_file
from src.stages import STAGE_NAMES, STAGES

log = logging.getLogger("src.pipeline")

IDEMPOTENCY_RUNS = ("corrida_1", "corrida_2")
IDEMPOTENCY_FILE = "idempotency.json"
IDEMPOTENCY_LOG = "idempotency.log"


def _java_version():
    java_home = os.environ.get("JAVA_HOME")
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    try:
        result = subprocess.run([java, "-version"], capture_output=True, text=True, check=False)
    except OSError:
        return "no encontrado"
    lines = (result.stderr or result.stdout).splitlines()
    return lines[0] if lines else "desconocido"


def _pyspark_version():
    try:
        return version("pyspark")
    except PackageNotFoundError:
        return "no instalado"


def _log_environment():
    log.info("entorno: Python %s | %s | PySpark %s", platform.python_version(), _java_version(), _pyspark_version())


def _reset_cassandra():
    from src.serving.cassandra import connect

    try:
        cluster, session = connect(config.CASSANDRA_HOST)
    except Exception as exc:
        log.warning("Cassandra no responde en %s (%s): no se borró el keyspace", config.CASSANDRA_HOST, type(exc).__name__)
        return
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {config.CASSANDRA_KEYSPACE}")
        log.info("borrado el keyspace %s", config.CASSANDRA_KEYSPACE)
    finally:
        cluster.shutdown()


def reset():
    landing = config.LANDING.resolve()
    for zone in (z.resolve() for z in config.DERIVED_ZONES):
        if zone == landing or zone in landing.parents:
            raise RuntimeError(f"reset no puede borrar {zone}: contiene Landing")
        if zone.exists():
            shutil.rmtree(zone)
            log.info("borrada %s", zone)
    _reset_cassandra()
    log.info("reset completo; Landing intacta en %s", landing)


def run(selected):
    _log_environment()
    for name, stage_fn in selected:
        try:
            with stage(name) as record:
                stage_fn(record)
        except Exception:
            log.error("corrida abortada en la etapa %s", name)
            return 1
    log.info("corrida completa: %s", " -> ".join(name for name, _ in selected))
    return 0


def _stream_rows_read(evidence_dir):
    record = json.loads((evidence_dir / "bronze_stream.json").read_text(encoding="utf-8"))
    return record["extra"]["input_rows"]


def _stored_state():
    from src.serving.cassandra import connect
    from src.spark import get_spark

    cluster, session = connect(config.CASSANDRA_HOST)
    try:
        return snapshot(get_spark(), session)
    finally:
        cluster.shutdown()


def _log_comparison(result):
    for zone, values in result["second"].items():
        first = result["first"].get(zone, {})
        log.info("%-40s %s", zone, " | ".join(f"{k}: {first.get(k)} -> {v}" for k, v in values.items()))
    log.info("lectura del stream en la corrida 2: %d filas", result["second_run_stream_rows"])
    for difference in result["differences"]:
        log.error("diferencia: %s", difference)
    log.info("chequeos: %s", result["checks"])


def idempotency():
    base = config.EVIDENCE
    runs = [base / name for name in IDEMPOTENCY_RUNS]
    for path in runs:
        shutil.rmtree(path, ignore_errors=True)
    reset()

    states = []
    try:
        for path in runs:
            config.EVIDENCE = path
            log_to_file(path / "run.log", mode="w")
            log.info("idempotencia: %s", path.name)
            if run(STAGES):
                return 1
            states.append(_stored_state())
    finally:
        config.EVIDENCE = base
        log_to_file(base / IDEMPOTENCY_LOG)

    result = compare(*states, _stream_rows_read(runs[1]))
    (base / IDEMPOTENCY_FILE).write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    _log_comparison(result)
    log.info("idempotencia: %s, detalle en %s", result["result"], IDEMPOTENCY_FILE)
    return 0 if result["result"] == "ok" else 1


def _select_stages(args):
    if args.stage:
        return [s for s in STAGES if s[0] == args.stage]
    if args.from_stage:
        return STAGES[STAGE_NAMES.index(args.from_stage):]
    return STAGES


def _parse_args(argv):
    parser = argparse.ArgumentParser(prog="python -m src.pipeline", description="Cloud Provider Analytics")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="ejecuta las etapas en orden")
    which = run_parser.add_mutually_exclusive_group()
    which.add_argument("--stage", choices=STAGE_NAMES, help="ejecuta solo esta etapa")
    which.add_argument("--from", dest="from_stage", choices=STAGE_NAMES, help="ejecuta desde esta etapa")

    commands.add_parser("reset", help="borra Bronze, Silver, Gold, quarantine, checkpoints y el keyspace de Cassandra")
    commands.add_parser("idempotency", help="reset, dos corridas completas y comparación del estado guardado")
    return parser.parse_args(argv)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    args = _parse_args(argv)
    if args.command == "idempotency":
        configure_logging(config.EVIDENCE / IDEMPOTENCY_LOG, config.LOG_LEVEL, mode="w")
    else:
        configure_logging(config.RUN_LOG, config.LOG_LEVEL)
    log.info("comando: %s", " ".join(argv))

    if args.command == "reset":
        reset()
        return 0
    if args.command == "idempotency":
        return idempotency()
    return run(_select_stages(args))


if __name__ == "__main__":
    sys.exit(main())
