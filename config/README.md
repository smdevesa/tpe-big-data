# config

Configuración externalizada por variables de entorno, leídas en [`src/config.py`](../src/config.py). Todas tienen un valor por defecto relativo al repositorio, así que en Google Colab no hay que definir nada. [`.env.example`](.env.example) lista las variables disponibles.

Cassandra corre local dentro del runtime (sin usuario ni token): el repositorio no contiene credenciales.
