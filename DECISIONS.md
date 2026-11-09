# Decisiones

Decisiones técnicas del proyecto y su justificación. Las decisiones abiertas de la primera entrega están en la sección 10 de [`docs/entrega1/diseno.md`](docs/entrega1/diseno.md).

- **Patrón híbrido.** Batch para maestros y facturación, Structured Streaming para eventos. Kappa no aporta porque las fuentes son archivos y no un log reproducible; Lambda duplicaría la agregación de eventos.
- **Entorno oficial: Google Colab** (Python 3.13+, Java 17, PySpark 4.2.1). Todo se ejecuta desde `notebooks/e2_pipeline_colab.ipynb`, sin pasos manuales.
- **Entrypoint único** `python -m src.pipeline run [--stage X | --from X]` y `reset`. El notebook no contiene lógica; el código se prueba con `pytest`.
- **Evidencia automática** por etapa en `evidence/e2/<etapa>.json` y `run.log`, generada por la misma corrida.
- **Serving en Apache Cassandra 5.0** single-node dentro del runtime de Colab, en lugar de AstraDB, que exige credenciales. La carga usa `cassandra-driver` con `foreachPartition` y prepared statements porque el Spark Cassandra Connector solo soporta Spark 3.5. `INSERT` es upsert por clave primaria, así que la carga es idempotente.
- **Cuatro fuentes batch en Bronze:** `customers_orgs`, `users`, `resources` y `billing_monthly`. `resources` se agrega porque Silver lo necesita para enriquecer eventos.
- **Corrige E1: maestros particionados por `ingest_date`** y facturación por `month`. La consigna pide Parquet particionado y coincide con la ingesta batch diaria.
- **Corrige E1: foto diaria en lugar de upsert para maestros.** Los CSV son extractos completos y Parquet no permite actualizar filas (un upsert real requiere Delta Lake o Iceberg). Re-ejecutar reescribe solo la partición del día, se conserva la historia para SCD y las bajas quedan reflejadas. El upsert se mantiene en eventos (`event_id`) y en Cassandra.
- **Esquema explícito y `try_cast`.** Los CSV se leen como texto con esquema explícito y se castean con `try_cast`; el encabezado se valida contra el esquema. Un valor que no castea no aborta la corrida y un cambio de columnas en la fuente detiene la etapa.
- **Quarantine en Bronze** para filas malformadas, clave o partición nula y casts fallidos, con el motivo y el registro original. Se controla `landing = bronze + quarantine + duplicados exactos` y la etapa falla si no cierra.
- **Deduplicación determinística.** Los duplicados exactos se descartan y se cuentan; si una clave se repite con contenido distinto se conserva siempre la misma fila y el resto va a quarantine (`duplicate_key`).
- **Costos negativos:** `cost_usd_increment >= -0.01` es válido; `< -0.01` se conserva en Silver con flag de anomalía y una muestra va a quarantine.
- **`unit` y `value`:** `unit` nulo se infiere de `metric` (cada métrica tiene una sola unidad); `value` nulo donde la métrica lo requiere va a quarantine. Imputar cero falsearía requests y consumo.
- **Streaming:** watermark amplio, dedupe por `event_id`, late data contada y logueada, checkpoint y trigger `availableNow`. Cada archivo trae unos 60 días desordenados, así que un watermark corto descartaría eventos válidos.
- **Anomalías de costo:** z-score robusto (MAD) de `daily_cost_usd` por organización y servicio, con umbral |z| > 3,5. La mediana y el MAD no se distorsionan por los propios spikes.
- **Diferido a la entrega final:** eventos anteriores a la creación del recurso, `credits` vacío, subtotales negativos, moneda y tipo de cambio, y escala del CSAT.
