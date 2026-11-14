import re
from datetime import datetime, timezone

from src import config

DEFAULT_KEYSPACE = "cloud_analytics"
LOCAL_DC = "datacenter1"
PROTOCOL_VERSION = 5
CONCURRENCY = 50
REQUEST_TIMEOUT_S = 60


def connect(host):
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    cluster = Cluster(
        [host],
        protocol_version=PROTOCOL_VERSION,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=LOCAL_DC),
    )
    session = cluster.connect()
    session.default_timeout = REQUEST_TIMEOUT_S
    return cluster, session


def _with_keyspace(text, keyspace):
    return re.sub(rf"\b{DEFAULT_KEYSPACE}\b", keyspace, text)


def _parse(path, keyspace):
    statements, name, lines = [], None, []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            if stripped.startswith("-- Consulta"):
                name = stripped.lstrip("- ")
            continue
        lines.append(stripped)
        if stripped.endswith(";"):
            statements.append((name, _with_keyspace(" ".join(" ".join(lines).rstrip(";").split()), keyspace)))
            name, lines = None, []
    return statements


def read_statements(path, keyspace):
    return [statement for _, statement in _parse(path, keyspace)]


def read_named_queries(path, keyspace):
    return _parse(path, keyspace)


def apply_schema(session, keyspace):
    for statement in read_statements(config.CQL_DIR / "schema.cql", keyspace):
        session.execute(statement)


def _cassandra_value(value):
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.astimezone(timezone.utc)
    return value


def partition_loader(host, keyspace, table, columns):
    insert = f"INSERT INTO {keyspace}.{table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})"

    def load(rows):
        from cassandra.concurrent import execute_concurrent_with_args

        values = [tuple(_cassandra_value(row[column]) for column in columns) for row in rows]
        if not values:
            return
        cluster, session = connect(host)
        try:
            execute_concurrent_with_args(
                session, session.prepare(insert), values, concurrency=CONCURRENCY, raise_on_first_error=True
            )
        finally:
            cluster.shutdown()

    return load


def count_rows(session, keyspace, table):
    return session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table}").one()[0]
