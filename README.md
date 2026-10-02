# Cloud Provider Analytics — TPE Big Data

Proyecto integrador de **72.80 Big Data** (ITBA, 2.º cuatrimestre 2026): pipeline ETL + streaming + serving
con PySpark, Parquet y Cassandra/AstraDB para analítica de FinOps, Soporte y Producto.

**Equipo:** Francisco Varela (64447) · Santiago Manuel Devesa (64223)
**Docente:** Diego Mosquera

| Instancia | Fecha límite |
|---|---|
| Primera evaluación parcial | Lunes 05/10/2026 (prórroga de una semana sobre el 28/09) |
| Segunda evaluación parcial | Lunes 16/11/2026 · 18:30 h |
| Evaluación final (MVP) | Lunes 07/12/2026 · 21:30 h |

## Estructura del repositorio

```
.
├── datalake/               Data Lake (ver datalake/README.txt)
│   └── landing/            datos crudos provistos por la cátedra — INMUTABLES, no modificar
│                           (bronze/, silver/, gold/ y quarantine/ se generan al ejecutar el pipeline y no se versionan)
├── docs/
│   ├── Consigna.pdf        consigna oficial de las tres instancias
│   ├── entrega1/           informe de la primera entrega (diseno.md + img/ + diagramas/)
│   └── build/              PDFs generados (ignorado por Git: se reconstruyen desde el Markdown)
└── tools/
    └── docs/               herramientas para generar el PDF desde Markdown (ver su README)
```

A medida que avance la implementación se irán sumando `notebooks/`, `src/`, `cql/` y `tests/`.

## Generar el PDF del informe

Requiere Python 3.10+. Desde la raíz del repo:

```bash
python -m venv .venv
source .venv/bin/activate              # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r tools/docs/requirements.txt
python tools/docs/build_pdf.py         # -> docs/build/entrega1.pdf
```

Detalle, convenciones de escritura y solución de problemas en [`tools/docs/README.md`](tools/docs/README.md).

## Convenciones

- El informe se escribe en Markdown (`docs/entrega1/diseno.md`); el PDF es un producto del build.
- `datalake/landing/` no se modifica nunca. 
- Al entregar: `git tag entregaN`.
- Nunca se versionan credenciales de AstraDB (token, Secure Connect Bundle): usar `.env` (ignorado).
