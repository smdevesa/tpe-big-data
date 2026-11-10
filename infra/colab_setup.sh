#!/usr/bin/env bash
# Prepara un runtime limpio de Google Colab. Idempotente.
set -euo pipefail
cd "$(dirname "$0")/.."

JAVA_HOME_17=/usr/lib/jvm/java-17-openjdk-amd64

if [ ! -x "$JAVA_HOME_17/bin/java" ]; then
  echo "Instalando OpenJDK 17..."
  apt-get -qq update
  apt-get -qq install -y openjdk-17-jdk-headless > /dev/null
fi

echo "Instalando dependencias de Python..."
pip install -q -r requirements.txt

python --version
"$JAVA_HOME_17/bin/java" -version 2>&1 | head -1
python -c "import pyspark; print('PySpark', pyspark.__version__)"

JAVA_HOME_17="$JAVA_HOME_17" bash infra/cassandra_setup.sh
