-- Consulta 1: costos y requests diarios por organización y servicio en un rango de fechas.
SELECT usage_date, daily_cost_usd, requests, events, events_missing_value, is_cost_anomaly, cost_robust_z
FROM cloud_analytics.usage_by_org_service_day
WHERE org_id = ? AND service = ? AND usage_date >= ? AND usage_date <= ?;

-- Consulta 2: top-N servicios por costo acumulado en los últimos 14 días para una organización.
SELECT rank, service, cost_14d_usd, days_with_usage, window_start, as_of_date
FROM cloud_analytics.top_services_14d
WHERE org_id = ? AND as_of_date = ?
LIMIT ?;
