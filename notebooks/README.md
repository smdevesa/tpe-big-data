# Notebooks de exploración

| Notebook | Contenido |
|---|---|
| `01_exploracion_maestros.ipynb` | Clientes, usuarios, recursos, soporte, marketing y facturación: esquema, nulos, calidad e integridad referencial |
| `02_exploracion_eventos.ipynb` | Stream de eventos (JSONL): volumen, versiones de esquema, tipos, outliers, orden temporal y llegada tardía |

Las salidas están guardadas, por lo que pueden leerse en GitHub sin ejecutar nada. Usan pandas sobre `datalake/landing/` (solo lectura).

## Ejecutar

```bash
pip install -r requirements.txt
jupyter lab notebooks/
```

El notebook busca `datalake/landing` hacia arriba desde el directorio actual; se puede forzar con la variable de entorno `LANDING_DIR`.

Los borradores personales van en `notebooks/scratch/` (ignorado por Git).
