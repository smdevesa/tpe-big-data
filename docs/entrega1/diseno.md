<div align="center">

<img src="img/logo-itba.png" alt="ITBA" width="240">

# 72.80 - Big Data

**Primera evaluación parcial**

**Alumnos**

| Nombre | Legajo |
|---|---|
| Santiago Manuel Devesa | 64223 |
| Rosario Otegui | — |
| Alicia Cobo Iglesias | — |
| Ernest Elies Domingo | — |
| Francisco Varela | 64447 |

**Profesores**

Diego Mosquera Uzcaregui

</div>

<!-- pagebreak -->

## 1. Problema, usuarios y objetivos

### Descripción del problema

El proveedor de nube dispone de datos de clientes, recursos, facturación, soporte y eventos de uso. Estas fuentes tienen distintas estructuras y frecuencias de actualización: los eventos de consumo requieren seguimiento cercano al tiempo real, mientras que los datos maestros y la facturación se procesan de forma diaria o mensual.

Los datos también presentan nulos, tipos ambiguos, valores inconsistentes y cambios de esquema. Estas condiciones dificultan su integración y pueden afectar la confiabilidad de los indicadores.

Se propone diseñar una solución que integre estas fuentes y permita consultar información consistente sobre costos, uso de servicios y atención al cliente. En esta primera entrega se define la arquitectura y se realiza una exploración inicial de los datos.

### Usuarios y preguntas principales

| Usuarios | Preguntas que necesitan responder |
|---|---|
| FinOps | ¿Cuál es el costo y la cantidad de requests diarios por organización y servicio? ¿Qué costos son anómalos? ¿Qué servicios acumulan más costo en los últimos 14 días? ¿Cuál es la facturación mensual considerando créditos, impuestos y conversión a USD? |
| Soporte | ¿Cómo evoluciona la cantidad de tickets críticos? ¿Qué proporción incumple el SLA? ¿Cuál es la satisfacción promedio de los clientes atendidos? |
| Producto / Usage | ¿Cómo varía el uso de cada servicio? ¿Cuántos tokens de GenAI se consumen por día y organización, y a qué costo estimado? ¿Qué métricas de carbono están disponibles? |

### Objetivos y criterios de éxito

| Objetivo | Criterio medible propuesto |
|---|---|
| Integrar las fuentes del caso | Contemplar las ocho fuentes provistas en el diseño, identificando su destino y su modalidad de ingesta. |
| Permitir el análisis de costos y consumo | Obtener costos y requests diarios por organización y servicio, y un ranking de servicios por costo acumulado en períodos de 14 días. |
| Permitir el seguimiento de soporte | Obtener conteos diarios de tickets por organización y severidad, tasa de incumplimiento de SLA y CSAT promedio de las respuestas disponibles. |
| Detectar anomalías de costo | Marcar con un flag o score los costos atípicos por organización, fecha y servicio con un método justificado (z-score robusto o percentiles), conservando el dato original. |
| Incorporar las métricas de Producto | Obtener tokens de GenAI diarios por organización y métricas de carbono cuando los campos estén presentes, distinguiendo ausencia de dato de valor cero. |
| Mantener la trazabilidad y controlar la calidad | Diseñar mecanismos para identificar el archivo de origen y la fecha de ingesta de cada registro, y contabilizar los registros que incumplan las reglas de calidad. |

Los indicadores se implementarán y validarán en las siguientes etapas. En esta primera entrega se evaluará que el diseño explique qué datos y componentes permitirán obtenerlos. La latencia objetivo del procesamiento cercano al tiempo real queda pendiente de definir y validar mediante pruebas.

## 2. Justificación de la necesidad de Big Data

El caso combina eventos de uso de servicios cloud con información de clientes, facturación y soporte. La necesidad de una arquitectura Big Data surge de integrar estas fuentes, procesarlas con diferentes frecuencias y mantener resultados confiables a medida que crece la operación.

El dataset provisto permite simular este escenario. Su tamaño, por sí solo, no demuestra la necesidad de procesamiento distribuido; la propuesta contempla el crecimiento esperado de un proveedor de nube y los requisitos del proyecto.

| Dimensión | Aplicación al caso | Implicación para el diseño |
|---|---|---|
| **Volumen** | El dataset provisto contiene 43.200 eventos (≈13 MB) en 60 días; en producción se acumulan a medida que aumentan los clientes, los recursos y el tiempo de operación. | Prever almacenamiento y procesamiento que puedan crecer, utilizando Parquet y particiones que permitan consultar solo los datos necesarios. |
| **Velocidad** | El seguimiento operativo de uso y costos requiere actualizaciones cercanas al tiempo real. Los maestros y la facturación tienen ciclos diarios o mensuales. | Combinar streaming para eventos con procesamiento batch para las demás fuentes. |
| **Variedad** | Se reciben archivos CSV de distintas áreas y eventos JSONL. Además, la segunda versión de los eventos incorpora nuevos campos. | Definir esquemas por fuente y compatibilizar sus estructuras antes de construir indicadores comunes. |
| **Veracidad** | Existen nulos, números representados como texto, costos negativos y valores atípicos que requieren interpretación. | Conservar los originales y establecer controles de calidad, reglas de tratamiento y trazabilidad. |
| **Valor** | Los datos permiten analizar costos, consumo, facturación, cumplimiento de SLA y uso de productos. | Organizar las salidas según las preguntas de FinOps, Soporte y Producto, con indicadores y niveles de detalle definidos. |

La arquitectura debe responder tanto al crecimiento de los datos como a la necesidad de combinar información histórica con eventos recientes. Para esta etapa se prioriza un diseño viable con las tecnologías previstas por la cátedra, sin incorporar infraestructura adicional que el caso no justifique.

## 3. Inventario y calidad inicial

[Link al Colab](https://colab.research.google.com/drive/1as-zul5f7WTrQ2JBLxVz6qLNNwRzgz8C)

La exploración se replica con pandas en los notebooks del repositorio: `notebooks/01_exploracion_maestros.ipynb` y `notebooks/02_exploracion_eventos.ipynb`, con salidas guardadas.

Se realizó una exploración en Google Colab con PySpark para revisar la estructura, la cantidad de registros, los valores nulos y los duplicados de las fuentes. Los archivos originales no fueron modificados.

| Fuente | Filas | Grano y clave candidata |
|---|---:|---|
| `customers_orgs.csv` | 80 | Organización; `org_id` |
| `users.csv` | 800 | Usuario; `user_id` |
| `resources.csv` | 400 | Recurso; `resource_id` |
| `support_tickets.csv` | 1.000 | Ticket; `ticket_id` |
| `marketing_touches.csv` | 1.500 | Interacción de marketing; `touch_id` |
| `nps_surveys.csv` | 92 | Encuesta de organización y fecha; `org_id` + `survey_date` |
| `billing_monthly.csv` | 240 | Factura; `invoice_id` |
| `usage_events_stream/*.jsonl` | 43.200 | Evento de una métrica de uso; `event_id`. Distribuidos en 120 archivos |

No se observaron duplicados en las claves revisadas. Este resultado corresponde al dataset provisto y no garantiza la unicidad en futuras ingestas. El campo `org_id` permite relacionar las fuentes con las organizaciones, mientras que `resource_id` vincula los eventos con los recursos. La integridad referencial entre fuentes (`org_id`, `resource_id`) fue verificada en los notebooks de exploración sobre el dataset provisto.

Se propone una ingesta batch diaria para clientes, usuarios, recursos, soporte, marketing y encuestas, y mensual para facturación. Estas frecuencias son decisiones de diseño: no pueden inferirse de los archivos estáticos del ZIP. Los eventos JSONL se utilizarán para simular una ingesta en micro-lotes.

Las fuentes combinan identificadores, categorías, fechas, booleanos e importes. En la exploración se utilizaron tipos inferidos por Spark; para la implementación se definirán esquemas explícitos. El campo `value` de los eventos fue interpretado como texto y requerirá una conversión antes de realizar cálculos. El campo `tags_json` contiene etiquetas que requieren interpretación como JSON.

### Hallazgos de calidad

| Hallazgo comprobado | Tratamiento propuesto para implementar después |
|---|---|
| 11 organizaciones sin `nps_score` | Calcular indicadores sobre puntuaciones disponibles e informar su cobertura |
| 139 usuarios sin `last_login` | Revisar el significado de la ausencia antes de clasificar su actividad |
| 83 recursos sin `tags_json` | Conservar la ausencia y considerar su efecto en análisis por etiquetas |
| 240 tickets sin `resolved_at` y 254 sin `csat` | Distinguir ausencias esperables de errores y calcular CSAT sobre respuestas disponibles |
| 19 encuestas sin `nps_score` y 10 sin `comment` | Diferenciar la ausencia de puntuación de la falta de comentario |
| 137 facturas sin `credits` | Acordar el significado del campo vacío antes de reemplazarlo por cero |
| 877 eventos sin `value` y 2.075 sin `unit` | Definir validaciones según la métrica y cómo tratar los registros incompletos |
| 216 eventos con costo negativo (211 por debajo de −0,01) | Marcar los casos y determinar si representan ajustes válidos o errores |
| 2.038 eventos con `unit` nulo pero con `value` | Inferir la unidad según servicio y métrica o marcar el registro |
| 1.309 `value` informados como texto, convertibles a número | Normalizar tipos en Silver y cuarentenar los no convertibles |
| 7.371 eventos con fecha anterior a la creación de su recurso | Definir si se rechazan o se conservan con un flag |
| Cada archivo del stream abarca ≈60 días de eventos | El orden de llegada no coincide con el de evento: el watermark no sirve para descartar datos; se usará upsert idempotente |
| 13 subtotales de factura negativos | Definir si son notas de crédito o errores |
| Tipo de cambio a USD distinto de 1 en 160 facturas; montos en ARS similares a los de USD | Verificar la moneda y la conversión antes de calcular ingresos |
| 172 tickets con `csat` sin `resolved_at` | Revisar la regla de coherencia entre ambos campos |
| `csat` con valores 0–7 y `nps_score` negativos | Escala no documentada: acordar el rango válido |
| 232 usuarios con `last_login` anterior a `created_at` | Regla de calidad de fechas |
| 10.800 eventos v1 y 32.400 eventos v2 | Compatibilizar las versiones mediante un esquema común |
| `carbon_kg` y `genai_tokens` ausentes en v1 | Mantener los campos como opcionales, sin reemplazar automáticamente su ausencia por cero |

> Los conteos de ausencias pueden superponerse: un mismo registro puede tener varios campos nulos. En la versión 2, `carbon_kg` está informado en 32.400 eventos y `genai_tokens` en 3.132.

### Trazabilidad y alcance de la exploración

Los originales se conservarán sin modificaciones en Landing. Para las etapas posteriores se propone incorporar `source_file` e `ingest_ts`, que identificarán el archivo de origen y el momento de ingesta. También se conservarán las claves de negocio y `schema_version` en los eventos.

La exploración revisa estructura, tipos, nulos, reglas de consistencia, integridad referencial y orden temporal de llegada, pero no constituye una limpieza: los criterios de tratamiento quedan como decisiones abiertas (sección 8).

## 4. Arquitectura de alto nivel

La solución tendrá dos caminos de entrada:

- **Batch:** PySpark leerá periódicamente los archivos CSV de clientes, usuarios, recursos, soporte, marketing, encuestas y facturación.
- **Streaming:** Structured Streaming leerá los eventos JSONL en micro-lotes para actualizar las métricas de uso y costos.

Ambos caminos utilizarán un Data Lake con cuatro zonas:

- **Landing:** archivos originales, sin modificaciones.
- **Bronze:** datos en Parquet, con tipos definidos e información de origen.
- **Silver:** datos normalizados, relacionados y con controles de calidad.
- **Gold:** tablas con indicadores para FinOps, Soporte y Producto.

Los resultados se publicarán en Cassandra/AstraDB para realizar consultas y visualizaciones. Durante todo el recorrido se contemplarán calidad, seguridad, trazabilidad, responsables y registro de errores.

Esta arquitectura es una propuesta de diseño; su implementación se realizará en las siguientes etapas.

[Link al Draw.io](https://app.diagrams.net/#G1EeB3nMbnvKgcZJeGUFQYVam3ngsjI1H7#%7B%22pageId%22%3A%22arquitectura-simple%22%7D)

![Cloud Provider Analytics — Arquitectura propuesta (v1)](img/arquitectura-v1.png)

*Fuente editable en texto: [`diagramas/arquitectura-v1.mmd`](diagramas/arquitectura-v1.mmd) (Mermaid).*

## 5. Selección del patrón arquitectónico

Se propone un **patrón híbrido**, que combina procesamiento batch y streaming según la frecuencia y la necesidad de actualización de cada fuente.

- **Batch:** para clientes, usuarios, recursos, soporte, marketing, encuestas y facturación. Estas fuentes se actualizan de forma periódica y no requieren procesar cada registro apenas llega.
- **Streaming:** para los eventos de uso, que permiten seguir el consumo y los costos con actualizaciones cercanas al tiempo real.

Ambos caminos utilizan las zonas del Data Lake y alimentan las tablas de negocio de Gold.

La elección permite atender las dos necesidades del caso sin convertir todas las fuentes en streams ni mantener dos implementaciones completas del mismo cálculo. Por eso se prefiere este enfoque frente a Kappa, centrado en streams, o una Lambda clásica, con caminos batch y de velocidad sobre los mismos eventos.

En la implementación académica, el streaming se simulará mediante la lectura de archivos JSONL en micro-lotes con Structured Streaming.

## 6. Mapeo de requisitos a componentes

La siguiente matriz relaciona las necesidades del caso con los componentes propuestos y las dimensiones de Big Data que justifican su elección.

| Requisito | Componente | Justificación y 5V |
|---|---|---|
| Cargar fuentes periódicas | PySpark batch | Integra los CSV. **Variedad.** |
| Procesar eventos de uso | Structured Streaming | Actualiza métricas en micro-lotes. **Velocidad.** |
| Conservar los originales | Landing | Permite rastrear y reprocesar datos. **Veracidad.** |
| Soportar el crecimiento | Spark y Parquet particionado | Organiza y procesa un histórico creciente. **Volumen.** |
| Definir tipos y origen | Bronze | Estandariza datos y registra su procedencia. **Variedad y veracidad.** |
| Normalizar y validar | Silver | Aplica reglas de calidad y compatibiliza versiones. **Veracidad.** |
| Generar indicadores | Gold | Responde preguntas de las áreas usuarias. **Valor.** |
| Consultar resultados | Cassandra/AstraDB y visualización | Facilita el acceso a los indicadores. **Valor.** |
| Controlar acceso y operación | Gobierno, seguridad y observabilidad | Define responsables y permite detectar errores. **Veracidad.** |

La matriz describe responsabilidades previstas en el diseño. Su funcionamiento se verificará durante las siguientes etapas de implementación.

## 7. Diseño del Data Lake

El Data Lake se organizará en cuatro zonas. Los datos avanzarán entre ellas a medida que cumplan las reglas definidas.

| Zona | Contenido | Formato | Regla de promoción |
|---|---|---|---|
| Landing | Archivos originales sin modificaciones | CSV y JSONL | Archivo legible y estructura reconocida |
| Bronze | Datos con tipos definidos y origen registrado | Parquet | Tipos y claves obligatorias válidos |
| Silver | Datos normalizados, relacionados y con versiones compatibles | Parquet | Reglas de calidad y relaciones necesarias verificadas |
| Gold | Indicadores para FinOps, Soporte y Producto | Parquet | Grano definido y cálculos verificados |

Los registros que incumplan las reglas de validez se guardarán por separado en **quarantine**, en Parquet, con el motivo del rechazo y su origen. No todo valor nulo o costo negativo se considerará automáticamente inválido.

### Particiones y nombres

Se propone particionar los eventos de Bronze y Silver por fecha del evento. En Gold, las tablas diarias se particionarán por fecha y las de facturación por mes. Los maestros pequeños permanecerán sin particionar para evitar archivos demasiado pequeños.

Los nombres usarán minúsculas y guiones bajos. Por ejemplo:

- `landing/customers_orgs.csv`
- `bronze/usage_events/event_date=2025-07-03/`
- `silver/usage_events/event_date=2025-07-03/`
- `gold/org_daily_usage_by_service/usage_date=2025-07-03/`

### Metadatos y trazabilidad

Bronze incorporará `source_file` e `ingest_ts`. Silver conservará esa información y, para los eventos, `schema_version`. En Gold se documentarán las fuentes utilizadas, la fecha de procesamiento y las reglas de cálculo.

### Retención

Para el alcance académico se propone conservar los originales y las evidencias durante todo el proyecto. Las versiones de trabajo de Bronze, Silver y Gold podrán reemplazarse mediante reprocesamientos controlados, evitando duplicados. Los registros en quarantine se conservarán hasta su revisión y cierre.

Una política de retención para producción quedará pendiente de definir según las necesidades del negocio y los costos de almacenamiento.

## 8. Flujos de procesamiento y plan inicial

**Batch:** PySpark leerá los CSV de Landing, generará Bronze y Silver en Parquet y calculará indicadores en Gold. Los resultados se cargarán en Cassandra/AstraDB para consultas y visualizaciones.

**Streaming:** Structured Streaming leerá los JSONL en micro-lotes y recorrerá las mismas zonas para actualizar indicadores de uso y costos. Se utilizarán checkpoints para recuperar el avance y controles por `event_id` para evitar duplicados.

**Ejemplo MapReduce:** para calcular el costo diario por organización y servicio, se propone:

- **Map:** tomar cada evento válido y emitir la clave `(organización, fecha, servicio)` junto con su costo.
- **Agrupar:** reunir los costos que tengan la misma clave.
- **Reduce:** sumar esos costos y guardar el total diario en Gold.

En PySpark, este cálculo se expresará mediante `groupBy` y `sum`.

**Supuestos y riesgos:** se asume que los archivos provistos representan las fuentes del caso y que los JSONL simulan eventos en llegada continua. Los principales riesgos son datos incompletos, cambios de esquema y duplicados por reprocesamiento. Se mitigarán con reglas de calidad, compatibilidad entre versiones y controles de claves.

**Decisiones abiertas.** Surgen de la exploración y se resolverán antes de implementar Silver:

| Tema | Hallazgo | Opciones a evaluar |
|---|---|---|
| Costos negativos | 216 eventos | Ajustes válidos, error a quarantine o conservar con flag |
| Llegada tardía | Cada archivo abarca ≈60 días; con un watermark de 7 días el 87,6 % de los eventos serían tardíos | No descartar por watermark; upsert idempotente por `event_id` |
| Eventos anteriores a la creación del recurso | 7.371 eventos (17,1 %) | Enviar a quarantine o conservar con flag |
| `unit` nulo con `value` | 2.038 eventos | Inferir la unidad según la métrica o marcar el registro |
| `credits` vacío | 137 facturas | Interpretarlo como cero o como dato faltante |
| Moneda y tipo de cambio | Tipo de cambio distinto de 1 en 160 facturas en USD; importes en ARS de magnitud similar a USD | Verificar con la cátedra y calcular ingresos en USD con la tasa de la factura |
| Escalas de NPS y CSAT | `csat` entre 0 y 7; `nps_score` negativos | Fijar el rango válido; los valores fuera de rango pasan a nulo con flag |
| Método de detección de anomalías de costo | Sin definir | Percentiles por servicio o z-score robusto por organización y servicio |
| Latencia objetivo del streaming | Sin definir | Fijarla tras pruebas |

## 9. Estimación de esfuerzo, roles y recursos

### Roles

| Integrante | Rol principal | Apoyo en |
|---|---|---|
| Santiago Manuel Devesa | A definir | A definir |
| Rosario Otegui | A definir | A definir |
| Alicia Cobo Iglesias | A definir | A definir |
| Ernest Elies Domingo | A definir | A definir |
| Francisco Varela | A definir | A definir |

### Estimación de esfuerzo

Horas-persona por paquete de trabajo. Las cifras son preliminares y se ajustarán al cerrar cada instancia.

| Paquete de trabajo | Resultado esperado | Responsable | Horas | Instancia |
|---|---|---|---:|---|
| Diseño y documento de arquitectura | Informe, diagrama y matriz requisito-componente | A definir | A definir | Primera entrega |
| Exploración y calidad de datos | Notebooks y reglas de calidad candidatas | A definir | A definir | Primera entrega |
| Repositorio y reproducibilidad | Estructura, README, dependencias y configuración | A definir | A definir | Primera entrega |
| Ingesta batch (Landing y Bronze) | Maestros y facturación en Parquet con origen registrado | A definir | A definir | Segunda entrega |
| Silver y quarantine | Datos normalizados, reglas de calidad y rechazados | A definir | A definir | Segunda entrega |
| Streaming de eventos | Micro-lotes, checkpoints y deduplicación por `event_id` | A definir | A definir | Segunda entrega |
| Gold (marts de negocio) | Tablas para FinOps, Soporte y Producto | A definir | A definir | Segunda entrega |
| Serving en Cassandra/AstraDB | Modelo por consulta y carga de resultados | A definir | A definir | Segunda entrega |
| Anomalías y calidad avanzada | Detección de costos atípicos y métricas de calidad | A definir | A definir | Entrega final |
| Pruebas | Pruebas de transformaciones y calidad | A definir | A definir | Entrega final |
| Defensa y demostración | Presentación, demo reproducible y evidencias | A definir | A definir | Entrega final |
| **Total** | | | **A definir** | |

### Recursos requeridos

| Recurso | Uso | Estado |
|---|---|---|
| PySpark y Structured Streaming | Procesamiento batch y streaming | A definir (entorno local o Colab) |
| Parquet y Data Lake local | Zonas Landing, Bronze, Silver y Gold | A definir |
| Cassandra/AstraDB | Serving de las consultas | A definir (cuenta y credenciales) |
| Repositorio Git | Versionado del código y del informe | Disponible |
| Google Colab y Jupyter | Exploración de datos | Disponible |
