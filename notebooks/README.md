# Notebooks

| Notebook | Contenido |
|---|---|
| `e2_pipeline_colab.ipynb` | **Ejecución oficial del pipeline en Google Colab**: clona el repositorio, instala el entorno, corre `python -m src.pipeline run` y las pruebas, y descarga la evidencia. No contiene lógica: todo está en `src/`. |
| `e1/01_exploracion_maestros.ipynb` | Exploración (primera entrega): clientes, usuarios, recursos, soporte, marketing y facturación; esquema, nulos, calidad e integridad referencial |
| `e1/02_exploracion_eventos.ipynb` | Exploración (primera entrega): stream de eventos (JSONL); volumen, versiones de esquema, tipos, outliers, orden temporal y llegada tardía |

## Exploración (`e1/`)

Las salidas están guardadas, por lo que pueden leerse en GitHub sin ejecutar nada. Usan pandas sobre `datalake/landing/` (solo lectura).

```bash
pip install -r requirements-dev.txt
jupyter lab notebooks/
```

El notebook busca `datalake/landing` hacia arriba desde el directorio actual; se puede forzar con la variable de entorno `LANDING_DIR`.

Los borradores personales van en `notebooks/scratch/` (ignorado por Git).
