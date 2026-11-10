#!/usr/bin/env bash
# Instala y levanta Apache Cassandra single-node en el runtime. Idempotente.
set -euo pipefail

CASSANDRA_VERSION="${CASSANDRA_VERSION:-5.0.4}"
CASSANDRA_DIR="${CASSANDRA_DIR:-/opt/cassandra}"
CASSANDRA_HOME="$CASSANDRA_DIR/apache-cassandra-$CASSANDRA_VERSION"
JAVA_HOME="${JAVA_HOME_17:-/usr/lib/jvm/java-17-openjdk-amd64}"

port_open() { (echo > /dev/tcp/127.0.0.1/9042) 2>/dev/null; }

if port_open; then
  echo "Cassandra ya está escuchando en 9042"
  exit 0
fi

if [ ! -x "$CASSANDRA_HOME/bin/cassandra" ]; then
  echo "Descargando Cassandra $CASSANDRA_VERSION..."
  mkdir -p "$CASSANDRA_DIR"
  curl -fsSL "https://archive.apache.org/dist/cassandra/$CASSANDRA_VERSION/apache-cassandra-$CASSANDRA_VERSION-bin.tar.gz" \
    | tar -xz -C "$CASSANDRA_DIR"
fi

echo "Iniciando Cassandra $CASSANDRA_VERSION..."
JAVA_HOME="$JAVA_HOME" MAX_HEAP_SIZE=1G HEAP_NEWSIZE=256M \
  "$CASSANDRA_HOME/bin/cassandra" -R > "$CASSANDRA_DIR/cassandra.log" 2>&1

for second in $(seq 5 5 300); do
  sleep 5
  if port_open; then
    echo "Cassandra lista en ${second}s"
    exit 0
  fi
done

echo "Cassandra no respondió en 300s; últimas líneas del log:"
tail -30 "$CASSANDRA_DIR/cassandra.log"
exit 1
