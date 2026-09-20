# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT - capa Silver
# MAGIC
# MAGIC Se lee la tabla Bronze (`kafka_bronze.trades_raw_kafka`), poblada por el
# MAGIC job de streaming clasico ya desplegado. No se requiere que Kafka ni el
# MAGIC productor local esten corriendo: se lee la tabla Delta que ya existe, y
# MAGIC el pipeline continua leyendo en modo streaming si Bronze vuelve a
# MAGIC recibir datos en el futuro.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

bronze_catalog = spark.conf.get("bronze_catalog")
bronze_schema = spark.conf.get("bronze_schema")
BRONZE_TABLE = f"{bronze_catalog}.{bronze_schema}.trades_raw_kafka"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpointing
# MAGIC
# MAGIC A diferencia del job de Bronze (Structured Streaming manual, donde se
# MAGIC declaro explicitamente un checkpointLocation dentro de un Volume), en DLT
# MAGIC el checkpoint no se configura de forma manual. Cada tabla declarada con
# MAGIC @dlt.table recibe su propio checkpoint interno, gestionado automaticamente
# MAGIC por el motor de DLT y ligado al pipeline. Ese checkpoint registra, para
# MAGIC cada micro-batch procesado, los offsets de origen leidos y el estado de
# MAGIC escritura correspondiente, de forma atomica. Es lo que permite que, si el
# MAGIC pipeline se detiene o falla y se vuelve a ejecutar, no se reprocesen datos
# MAGIC ya escritos ni se pierdan datos a medio procesar. El progreso puede
# MAGIC inspeccionarse desde la pestana "Event log" del pipeline en la UI, sin
# MAGIC necesidad de acceder a ningun archivo de checkpoint manualmente.
# MAGIC
# MAGIC ## Watermarking
# MAGIC
# MAGIC Se declara un watermark de 2 minutos sobre trade_timestamp. Esto establece
# MAGIC que el motor de Spark espera hasta 2 minutos por eventos que lleguen fuera
# MAGIC de orden antes de considerar cerrada una ventana de tiempo. Cualquier
# MAGIC evento cuyo trade_timestamp sea mas antiguo que (maximo_timestamp_visto -
# MAGIC 2 minutos) se descarta de forma silenciosa, y el estado interno asociado a
# MAGIC esa ventana se libera de memoria.

# COMMAND ----------

@dlt.table(
    name="trades_clean",
    comment="Trades limpios y deduplicados, con watermark de 2 minutos",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_fail(
    "valid_symbol_and_timestamp",
    "symbol IS NOT NULL AND trade_timestamp IS NOT NULL",
)
def trades_clean():
    raw_stream = (
        spark.readStream.table(BRONZE_TABLE)
        # price y quantity llegan como STRING desde Bronze. Sin este cast
        # explicito a DOUBLE, Spark intenta convertir implicitamente el
        # string al comparar contra el 0 de las expectativas, y en modo
        # ANSI elige BIGINT en vez de DOUBLE -- lo cual falla para
        # cualquier valor con decimales (ej: "81318.00000000") y hace que
        # expect_or_drop descarte la fila por error de conversion, no por
        # una violacion real de la regla de negocio.
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
    )

    return (
        raw_stream.withWatermark("trade_timestamp", "2 minutes")
        .dropDuplicatesWithinWatermark(["symbol", "trade_id"])
        .select(
            "symbol",
            "price",
            "quantity",
            "trade_timestamp",
            F.current_timestamp().alias("silver_processed_at"),
        )
    )