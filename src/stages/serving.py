import logging
import time
from datetime import date, datetime, timedelta

from cassandra.util import Date as CassandraDate
from pyspark.sql import functions as F

from src import config
from src.serving.cassandra import apply_schema, connect, count_rows, partition_loader, read_named_queries
from src.serving.ranking import WINDOW_DAYS, top_services
from src.spark import get_spark

log = logging.getLogger(__name__)

MART = "org_daily_usage_by_service"
USAGE_TABLE = "usage_by_org_service_day"
TOP_TABLE = "top_services_14d"
USAGE_COLUMNS = (
    "org_id", "service", "usage_date", "org_name", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
    "genai_tokens", "carbon_kg", "events", "events_missing_value", "cost_robust_z", "is_cost_anomaly", "processed_at",
)
TOP_COLUMNS = (
    "org_id", "as_of_date", "rank", "service", "cost_14d_usd", "days_with_usage", "window_start", "processed_at",
)
RANGE_DAYS = 7
TOP_N = 3
RESULTS_FILE = "cql_results.txt"


def _load(df, table, columns):
    start = time.perf_counter()
    loader = partition_loader(config.CASSANDRA_HOST, config.CASSANDRA_KEYSPACE, table, columns)
    df.select(*columns).coalesce(config.SPARK_SHUFFLE_PARTITIONS).foreachPartition(loader)
    return round(time.perf_counter() - start, 2)


def _jsonable(value):
    if isinstance(value, CassandraDate):
        value = value.date()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _run_query(session, name, cql, params):
    start = time.perf_counter()
    rows = list(session.execute(session.prepare(cql), params))
    return {
        "name": name,
        "cql": cql,
        "params": [_jsonable(param) for param in params],
        "rows": [{key: _jsonable(value) for key, value in row._asdict().items()} for row in rows],
        "duration_ms": round(1000 * (time.perf_counter() - start), 1),
    }


def _query_params(latest):
    example = config.EVIDENCE_EXAMPLE
    until = date.fromisoformat(example["date"])
    return [
        (example["org_id"], example["service"], until - timedelta(days=RANGE_DAYS - 1), until),
        (example["org_id"], latest, TOP_N),
    ]


def _format_results(queries):
    lines = []
    for query in queries:
        lines += [query["name"], query["cql"], f"parámetros: {query['params']}", ""]
        if query["rows"]:
            headers = list(query["rows"][0])
            table = [headers] + [[str(row[h]) for h in headers] for row in query["rows"]]
            widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
            lines += [" | ".join(cell.ljust(width) for cell, width in zip(row, widths)) for row in table]
        lines += [f"({len(query['rows'])} filas, {query['duration_ms']} ms)", "", ""]
    return "\n".join(lines)


def run(record):
    spark = get_spark()
    mart_path = config.GOLD / MART
    keyspace = config.CASSANDRA_KEYSPACE

    mart = spark.read.parquet(str(mart_path)).cache()
    ranking = top_services(mart).withColumn("processed_at", F.current_timestamp()).cache()
    sources = {USAGE_TABLE: (mart, USAGE_COLUMNS), TOP_TABLE: (ranking, TOP_COLUMNS)}
    latest = mart.agg(F.max("usage_date")).first()[0]

    cluster, session = connect(config.CASSANDRA_HOST)
    try:
        release = session.execute("SELECT release_version FROM system.local").one()[0]
        apply_schema(session, keyspace)

        tables = {}
        for table, (df, columns) in sources.items():
            rows_source = df.count()
            load_seconds = _load(df, table, columns)
            rows_cassandra = count_rows(session, keyspace, table)
            tables[table] = {"rows_source": rows_source, "rows_cassandra": rows_cassandra, "load_seconds": load_seconds}
            log.info("%s.%s: %d filas cargadas en %.1fs, %d en Cassandra", keyspace, table, rows_source, load_seconds, rows_cassandra)
            if rows_source != rows_cassandra:
                raise RuntimeError(f"{table}: {rows_source} filas de origen != {rows_cassandra} en Cassandra")

        named = read_named_queries(config.CQL_DIR / "queries.cql", keyspace)
        queries = [_run_query(session, name, cql, params) for (name, cql), params in zip(named, _query_params(latest))]
    finally:
        cluster.shutdown()
        mart.unpersist()
        ranking.unpersist()

    for query in queries:
        log.info("%s -> %d filas en %.1f ms", query["name"].split(":")[0], len(query["rows"]), query["duration_ms"])
    results_path = config.EVIDENCE / RESULTS_FILE
    results_path.write_text(_format_results(queries), encoding="utf-8")

    record.input(mart_path, tables[USAGE_TABLE]["rows_source"])
    record.output(results_path)
    record.extra.update({
        "cassandra_version": release,
        "keyspace": keyspace,
        "tables": tables,
        "window_days": WINDOW_DAYS,
        "latest_usage_date": latest.isoformat(),
        "queries": queries,
    })
