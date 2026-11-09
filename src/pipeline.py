import argparse
import logging
import os
import platform
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version

from src import config
from src.evidence import stage
from src.logs import configure_logging
from src.stages import STAGE_NAMES, STAGES

log = logging.getLogger("src.pipeline")


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


def reset():
    landing = config.LANDING.resolve()
    for zone in (z.resolve() for z in config.DERIVED_ZONES):
        if zone == landing or zone in landing.parents:
            raise RuntimeError(f"reset no puede borrar {zone}: contiene Landing")
        if zone.exists():
            shutil.rmtree(zone)
            log.info("borrada %s", zone)
    log.info("reset completo; Landing intacta en %s", landing)


def run(selected):
    _log_environment()
    pending = []
    for name, stage_fn in selected:
        try:
            with stage(name) as record:
                stage_fn(record)
        except Exception:
            log.error("corrida abortada en la etapa %s", name)
            return 1
        if record.status == "pending":
            pending.append(name)
    log.info("corrida completa: %s", " -> ".join(name for name, _ in selected))
    if pending:
        log.warning("etapas pendientes de implementación: %s", ", ".join(pending))
    return 0


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

    commands.add_parser("reset", help="borra Bronze, Silver, Gold, quarantine y checkpoints")
    return parser.parse_args(argv)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    args = _parse_args(argv)
    configure_logging(config.RUN_LOG, config.LOG_LEVEL)
    log.info("comando: %s", " ".join(argv))

    if args.command == "reset":
        reset()
        return 0
    return run(_select_stages(args))


if __name__ == "__main__":
    sys.exit(main())
