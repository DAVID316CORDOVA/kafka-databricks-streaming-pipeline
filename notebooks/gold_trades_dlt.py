# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT - capa Gold
# MAGIC
# MAGIC Se lee la tabla Silver (`silver.trades_clean`), publicada por el
# MAGIC pipeline de Silver como una tabla independiente de Unity Catalog. Este
# MAGIC pipeline se despliega y ejecuta por separado del de Silver: la lectura
# MAGIC se hace con spark.readStream.table (no con dlt.read), porque la tabla de
# MAGIC origen pertenece a otro pipeline, no a este.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

upstream_silver_catalog = spark.conf.get("upstream_silver_catalog")
upstream_silver_schema = spark.conf.get("upstream_silver_schema")
SILVER_TABLE = f"{upstream_silver_catalog}.{upstream_silver_schema}.trades_clean"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermarking en la capa Gold
# MAGIC
# MAGIC Se vuelve a declarar un watermark en esta etapa, aunque el pipeline de
# MAGIC Silver ya declaro el suyo. Cada etapa de agregacion streaming requiere su
# MAGIC propio watermark: es lo que le permite a esta agregacion en particular
# MAGIC determinar cuando una ventana de 1 minuto puede considerarse cerrada y
# MAGIC emitirse como resultado final.

# COMMAND ----------

@dlt.table(
    name="trades_ohlc_1min",
    comment="Velas OHLC de 1 minuto por simbolo",
    table_properties={"quality": "gold"},
)
def trades_ohlc_1min():
    silver_stream = spark.readStream.table(SILVER_TABLE)

    return (
        silver_stream.withWatermark("trade_timestamp", "2 minutes")
        .groupBy(
            F.window("trade_timestamp", "1 minute"),
            "symbol",
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        )
    )