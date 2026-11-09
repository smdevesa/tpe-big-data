# infra

Scripts y configuración para ejecutar el proyecto desde un entorno limpio. El entorno oficial es Google Colab (ver [`notebooks/e2_pipeline_colab.ipynb`](../notebooks/e2_pipeline_colab.ipynb)).

| Archivo | Qué hace |
|---|---|
| `colab_setup.sh` | Instala OpenJDK 17 y las dependencias de [`requirements.txt`](../requirements.txt) en el runtime de Colab. Idempotente. |
| `log4j2.properties` | Logs de Spark: nivel WARN, mismo formato que los del proyecto, sin avisos irrelevantes (`NativeCodeLoader`). Lo carga [`src/spark.py`](../src/spark.py). |
