-- Keyspace y tablas query-first para el mart org_daily_usage_by_service.
-- Un solo nodo: SimpleStrategy con replication_factor 1.

CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Consulta 1: costos y requests diarios por organización y servicio en un rango de fechas.
CREATE TABLE IF NOT EXISTS cloud_analytics.usage_by_org_service_day (
  org_id text,
  service text,
  usage_date date,
  org_name text,
  daily_cost_usd double,
  requests double,
  cpu_hours double,
  storage_gb_hours double,
  genai_tokens bigint,
  carbon_kg double,
  events int,
  events_missing_value int,
  cost_robust_z double,
  is_cost_anomaly boolean,
  processed_at timestamp,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Consulta 2: top-N servicios por costo acumulado en los 14 días que terminan en as_of_date.
CREATE TABLE IF NOT EXISTS cloud_analytics.top_services_14d (
  org_id text,
  as_of_date date,
  rank int,
  service text,
  cost_14d_usd double,
  days_with_usage int,
  window_start date,
  processed_at timestamp,
  PRIMARY KEY ((org_id, as_of_date), rank)
) WITH CLUSTERING ORDER BY (rank ASC);
