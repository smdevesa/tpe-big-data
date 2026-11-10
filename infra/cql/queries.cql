-- Consultas mínimas (sección 7.4 de la consigna). Se ejecutan como prepared statements desde
-- src/stages/serving.py; los parámetros de ejemplo y los resultados quedan en evidence/e2/.

-- Consulta 1: costos y requests diarios por organización y servicio en un rango de fechas.
-- Parámetros: org_id, service, fecha desde, fecha hasta.
SELECT usage_date, daily_cost_usd, requests, events, events_missing_value, is_cost_anomaly, cost_robust_z
FROM cloud_analytics.usage_by_org_service_day
WHERE org_id = ? AND service = ? AND usage_date >= ? AND usage_date <= ?;

-- Consulta 2: top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- Parámetros: org_id, as_of_date (fecha más reciente del mart), N.
SELECT rank, service, cost_14d_usd, days_with_usage, window_start, as_of_date
FROM cloud_analytics.top_services_14d
WHERE org_id = ? AND as_of_date = ?
LIMIT ?;
