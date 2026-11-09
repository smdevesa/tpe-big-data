import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DATALAKE = Path(os.environ.get("DATALAKE_DIR") or ROOT / "datalake")
LANDING = Path(os.environ.get("LANDING_DIR") or DATALAKE / "landing")
BRONZE = DATALAKE / "bronze"
SILVER = DATALAKE / "silver"
GOLD = DATALAKE / "gold"
QUARANTINE = DATALAKE / "quarantine"
CHECKPOINTS = DATALAKE / "_checkpoints"
DERIVED_ZONES = (BRONZE, SILVER, GOLD, QUARANTINE, CHECKPOINTS)

EVIDENCE = Path(os.environ.get("EVIDENCE_DIR") or ROOT / "evidence" / "e2")
RUN_LOG = EVIDENCE / "run.log"
LOG_LEVEL = os.environ.get("LOG_LEVEL") or "INFO"

SPARK_MASTER = os.environ.get("SPARK_MASTER") or "local[*]"

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST") or "127.0.0.1"
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE") or "cloud_analytics"
