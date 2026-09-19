#!/bin/bash
# Version EC2 de setup_kafka.sh: levanta Kafka + Kafka UI, y crea el topic
# 'trades-stream' con 3 particiones. Se corre EN el EC2, por SSH -- ahi no
# hace falta MSYS_NO_PATHCONV (eso es solo para Git Bash en Windows).
#
# Uso: bash setup_kafka_ec2.sh

set -e

echo "--- Levantando Kafka + Kafka UI ---"
docker compose -f docker-compose.yml up -d

echo "--- Esperando a que Kafka este listo (10s) ---"
sleep 10

echo "--- Creando topic 'trades-stream' con 3 particiones ---"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic trades-stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "--- Topics existentes ---"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

echo ""
echo "Listo. Kafka UI disponible en: http://54.242.249.122:8090"
echo "Ahi puedes ver el topic 'trades-stream', sus 3 particiones, y"
echo "cualquier consumer group que se conecte -- en tiempo real."
