# Decisiones

Decisiones técnicas del proyecto y su justificación.

- **Patrón híbrido.** Batch para los maestros, Structured Streaming para los eventos de uso. Kappa no aporta porque las fuentes son archivos y no un log reproducible; Lambda duplicaría la agregación de eventos.
- **Serving en Apache Cassandra 5.0** single-node dentro del runtime de Colab, en lugar de AstraDB, porque el equipo está más familiarizado con Cassandra. Keyspace `cloud_analytics` con `SimpleStrategy` y RF 1.
- **Particiones.** Maestros por `ingest_date`, facturación por `month`, eventos por `event_date` (Bronze y Silver) y Gold por `usage_date`.
- **Maestros como foto diaria, no upsert** (cambia el diseño de E1). Los CSV son extractos completos y Parquet no permite actualizar filas; re-ejecutar reescribe solo la partición del día, se conserva la historia y las bajas quedan reflejadas.
- **Esquema explícito y `try_cast`.** Todo se lee como texto y se castea: un valor que no se puede convertir no aborta la corrida. En Bronze batch esas filas van a quarantine con motivo y se verifica `Landing = Bronze + quarantine + duplicados`.
- **Watermark de 61 días.** Cada archivo mezcla eventos de los 60 días del dataset (92 % llega fuera de orden). Medido en Spark: con 7 días se descartan 31.811 de 43.200 eventos; con 61, ninguno. Los descartados se cuentan y se loguean.
- **Deduplicación por `event_id`.** `dropDuplicatesWithinWatermark` con un hash del registro como clave cuando falta el id, para no colapsar eventos inválidos. No se deduplica por contenido: hay eventos distintos con mismo recurso, minuto y métrica.
- **Silver rechaza solo lo inutilizable.** Va a quarantine el evento sin `event_id`, duplicado, con JSON roto, sin `timestamp`, `org_id` o `service`, o con `value` sin `unit` deducible. Lo sospechoso o incompleto se conserva con flag; nunca se imputa 0.
- **Costos negativos:** `cost_usd_increment >= -0.01` es válido; por debajo se conserva con `cost_anomaly` y se copia a quarantine como `flagged` (211 eventos).
- **`value` nulo se conserva con `value_missing`** (877 eventos). La consigna exige `unit` solo cuando existe `value`; descartarlo perdería el 2 % del costo.
- **`unit` nula se infiere de `metric`**, que tiene una sola unidad por métrica (2.075 eventos).
- **Anomalías de costo con z-score robusto (MAD)** de `daily_cost_usd` por organización y servicio, umbral |z| > 3,5. La mediana y el MAD no se distorsionan por los propios picos. Usa la serie completa: sirve para análisis retrospectivo; en operación continua corresponde una ventana móvil.
- **Claves de Cassandra.** `usage_by_org_service_day`: `((org_id, service), usage_date)`, una partición por consulta y rango de fechas secuencial. `top_services_14d`: `((org_id, as_of_date), rank)`, ranking de 14 días precalculado en Spark para cada fecha, porque Cassandra no ordena por una suma; `rank` en la clave evita duplicar un servicio si su costo cambia entre cargas.
- **Idempotencia verificada con un comando.** `python -m src.pipeline idempotency` hace `reset`, dos corridas y compara filas, claves distintas y sumas guardadas en cada zona y en Cassandra.
- **Diferido a la entrega final:** eventos anteriores a la creación del recurso (hoy solo flag), `credits` vacío, subtotales negativos, tipo de cambio y escala del CSAT.
