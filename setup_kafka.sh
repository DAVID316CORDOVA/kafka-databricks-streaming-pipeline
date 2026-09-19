#!/bin/bash
# Levanta Kafka + Kafka UI, y crea el topic 'trades-stream' con 3
# particiones (replication factor 1, porque solo hay 1 broker local).
#
# Uso: bash setup_kafka.sh

set -e

echo "--- Levantando Kafka + Kafka UI ---"
docker compose up -d

echo "--- Esperando a que Kafka este listo (10s) ---"
sleep 10

echo "--- Creando topic 'trades-stream' con 3 particiones ---"
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic trades-stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "--- Topics existentes ---"
MSYS_NO_PATHCONV=1 docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

echo ""
echo "Listo. Kafka UI disponible en: http://localhost:8090"
echo "Ahi puedes ver el topic 'trades-stream', sus 3 particiones, y"
echo "cualquier consumer group que se conecte -- en tiempo real."