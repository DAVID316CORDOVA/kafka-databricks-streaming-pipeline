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

dbutils.widgets.text("catalog", "streaming_project", "Unity Catalog catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("kafka_topic", "trades-stream", "Kafka topic")
dbutils.widgets.text("kafka_bootstrap_servers", "", "Kafka bootstrap servers (ngrok address)")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
kafka_topic = dbutils.widgets.get("kafka_topic")
kafka_bootstrap_servers = dbutils.widgets.get("kafka_bootstrap_servers")

target_table = f"{catalog}.{bronze_schema}.trades_raw"
checkpoint_path = f"/Volumes/{catalog}/{bronze_schema}/checkpoints/trades_raw"
# Ajusta checkpoint_path si no usas Unity Catalog Volumes -- alternativa:
# un path en DBFS, ej. "/tmp/checkpoints/trades_raw" (no recomendado para
# produccion real, pero funciona para practicar).

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
# MAGIC `kafka.bootstrap.servers` apunta a tu direccion publica de ngrok
# MAGIC (ej. "0.tcp.ngrok.io:12345"), no a "localhost:9092" -- Databricks
# MAGIC corre en la nube, "localhost" ahi seria el propio cluster, no tu PC.
# MAGIC
# MAGIC `startingOffsets: earliest` lee desde el principio del topic la
# MAGIC primera vez que corre; corridas posteriores retoman desde el
# MAGIC checkpoint, no desde el principio de nuevo.

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parseando el value (bytes JSON) + agregando watermark
# MAGIC
# MAGIC Kafka entrega `key` y `value` como bytes crudos -- hay que
# MAGIC decodificarlos y parsear el JSON manualmente, a diferencia de
# MAGIC Event Hub donde ya veniamos trabajando con el string directo.
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
# MAGIC exactamente donde se quedo, leyendo el checkpoint -- no hay que
# MAGIC gestionar manualmente un "ultimo offset visto" como si tocaba
# MAGIC hacer con `partition_context.update_checkpoint()` en la version de
# MAGIC Event Hub.
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