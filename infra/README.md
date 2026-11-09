# infra

Scripts para ejecutar el proyecto desde un entorno limpio. El entorno oficial es Google Colab (ver [`notebooks/e2_pipeline_colab.ipynb`](../notebooks/e2_pipeline_colab.ipynb)).

| Archivo | Qué hace |
|---|---|
| `colab_setup.sh` | Instala OpenJDK 17 y las dependencias de [`requirements.txt`](../requirements.txt) en el runtime de Colab. Idempotente. |
