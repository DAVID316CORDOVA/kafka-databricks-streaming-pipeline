# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Streaming Ingestion - Real Structured Streaming
# MAGIC
# MAGIC Unlike the Event Hub version of this project (which had to use a
# MAGIC manual Python loop because that workspace only allowed serverless
# MAGIC compute), this cluster is a classic all-purpose cluster -- so we
# MAGIC can use the REAL `readStream`/`writeStream` API. The Kafka
# MAGIC connector (`org.apache.spark:spark-sql-kafka-0-10`) ships built
# MAGIC into the Databricks Runtime by default, no manual JAR install
# MAGIC needed.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_fintech_fdcg01", "Unity Catalog catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("kafka_topic", "trades-stream", "Kafka topic")
dbutils.widgets.text("kafka_bootstrap_servers", "", "Kafka bootstrap servers (EC2 address)")
# NUEVO: controla desde donde empieza a leer la PRIMERA vez que corre.
# "earliest" = lee todo el historial que exista en el topic (usalo en dev,
# donde quieres reprocesar los datos que ya generaste).
# "latest"   = ignora el historial y solo lee mensajes que lleguen a partir
# de ahora (usalo en prod, para no reprocesar el backlog completo la
# primera vez que despliegues). Corridas posteriores, en cualquiera de
# los dos casos, retoman desde el checkpoint, no desde este valor de nuevo.
dbutils.widgets.text("starting_offsets", "earliest", "Starting offsets (earliest/latest)")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
kafka_topic = dbutils.widgets.get("kafka_topic")
kafka_bootstrap_servers = dbutils.widgets.get("kafka_bootstrap_servers")
starting_offsets = dbutils.widgets.get("starting_offsets")

print(f"DEBUG - kafka_bootstrap_servers recibido: '{kafka_bootstrap_servers}'")
print(f"DEBUG - kafka_topic recibido: '{kafka_topic}'")
print(f"DEBUG - starting_offsets recibido: '{starting_offsets}'")


target_table = f"{catalog}.{bronze_schema}.trades_raw_kafka"
checkpoint_path = f"/Volumes/{catalog}/{bronze_schema}/checkpoints/trades_raw"

# COMMAND ----------

from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType
)

trade_schema = StructType([
    StructField("event_type", StringType()),
    StructField("event_time_ms", LongType()),
    StructField("symbol", StringType()),
    StructField("trade_id", LongType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
    StructField("trade_time_ms", LongType()),
    StructField("is_buyer_market_maker", BooleanType()),
    StructField("ingested_at", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## readStream: conectando al topic de Kafka
# MAGIC
# MAGIC `kafka.bootstrap.servers` apunta a la IP publica del EC2 donde
# MAGIC corre el broker (ej. "54.242.249.122:9092"), no a "localhost:9092"
# MAGIC -- Databricks corre en la nube, "localhost" ahi seria el propio
# MAGIC cluster, no tu PC.
# MAGIC
# MAGIC `startingOffsets` ahora viene del widget: "earliest" en dev (lee
# MAGIC todo el historial), "latest" en prod (ignora el backlog y solo lee
# MAGIC lo nuevo). Corridas posteriores a la primera siempre retoman desde
# MAGIC el checkpoint, sin importar este valor.

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", starting_offsets)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parseando el value (bytes JSON) + agregando watermark
# MAGIC
# MAGIC Kafka entrega `key` y `value` como bytes crudos -- hay que
# MAGIC decodificarlos y parsear el JSON manualmente.
# MAGIC
# MAGIC El watermark le dice a Spark "no esperes datos de mas de 2 minutos
# MAGIC de atraso" -- pasado ese margen, el estado interno para ventanas
# MAGIC de agregacion se libera, para que la memoria del cluster no crezca
# MAGIC sin limite mientras el stream corre indefinidamente.

# COMMAND ----------

parsed_stream = (
    raw_stream
    .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value")
    .select(
        "kafka_key",
        from_json(col("json_value"), trade_schema).alias("data"),
    )
    .select("kafka_key", "data.*")
    .withColumn("trade_timestamp", to_timestamp(col("trade_time_ms") / 1000))
    .withWatermark("trade_timestamp", "2 minutes")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## writeStream: append a Delta, con checkpoint real
# MAGIC
# MAGIC `checkpointLocation` es lo que hace que esto sea resumible de
# MAGIC verdad: si el cluster se cae o el Job se reinicia, Spark retoma
# MAGIC exactamente donde se quedo, leyendo el checkpoint.
# MAGIC
# MAGIC `trigger(processingTime="30 seconds")` hace que el micro-batch
# MAGIC corra cada 30s en vez de tan rapido como pueda -- mas facil de
# MAGIC observar mientras aprendes, y mas barato en costo de cluster.

# COMMAND ----------

query = (
    parsed_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .toTable(target_table)
)

query.awaitTermination()