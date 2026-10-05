# Cloud Provider Analytics — TPE Big Data

Proyecto integrador de **72.80 Big Data** (ITBA, 2.º cuatrimestre 2026).

**Equipo:** 
- Santiago Manuel Devesa (64223) 
- Rosario Otegui (63708) 
- Alicia Cobo Iglesias (69568) 
- Ernest Elies Domingo (69613) 
- Francisco Varela (64447)

**Docente:** Prof. (Adj.) Diego Mosquera

**Estado actual del repositorio:** Primera entrega parcial

## Objetivo

Pipeline ETL + streaming + serving para un proveedor de nube: integra clientes, recursos, facturación, soporte, marketing y eventos de uso para responder preguntas de FinOps, Soporte y Producto. Usa PySpark, Parquet y Cassandra/AstraDB.

## Arquitectura

Patrón híbrido (batch + streaming) sobre un Data Lake de cuatro zonas en Parquet: Landing → Bronze → Silver → Gold, con publicación en Cassandra/AstraDB.

![Arquitectura propuesta](docs/entrega1/img/arquitectura-v1.1.png)

## Requisitos

- Python 3.10+ (dependencias en [`requirements.txt`](requirements.txt))

## Ejecución

```bash
python -m venv .venv
source .venv/bin/activate              # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

| Qué | Cómo |
|---|---|
| Generar el PDF del informe | `python tools/docs/build_pdf.py` → `docs/build/entrega1.pdf` 
| Explorar los datos | `jupyter lab notebooks/` (ver [`notebooks/README.md`](notebooks/README.md)) |

El comando para ejecutar el pipeline se agregará con la implementación.

## Pruebas

Se agregarán en `tests/` junto con el código para la segunda entrega.

## Limitaciones

No hay pipeline implementado todavía; los indicadores y las reglas de calidad del informe son propuestas.
