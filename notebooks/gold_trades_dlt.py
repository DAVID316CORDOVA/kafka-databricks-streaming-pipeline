# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT - capa Gold
# MAGIC
# MAGIC Se lee la tabla Silver (`silver.trades_clean`), publicada por el
# MAGIC pipeline de Silver como una tabla independiente de Unity Catalog. Este
# MAGIC pipeline se despliega y ejecuta por separado del de Silver: la lectura
# MAGIC se hace con spark.readStream.table (no con dlt.read), porque la tabla de
# MAGIC origen pertenece a otro pipeline, no a este.

import dlt
from pyspark.sql import functions as F

SILVER_TABLE = "dbw_fintech_fdcg01.silver.trades_clean"


# ---------------------------------------------------------------------------
# WATERMARKING en la capa Gold
# ---------------------------------------------------------------------------
# Se vuelve a declarar un watermark en esta etapa, aunque el pipeline de
# Silver ya declaro el suyo. Cada etapa de agregacion streaming requiere su
# propio watermark: es lo que le permite a esta agregacion en particular
# determinar cuando una ventana de 1 minuto puede considerarse cerrada y
# emitirse como resultado final, en vez de permanecer abierta de forma
# indefinida a la espera de mas datos.

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
            F.col("symbol"),
            F.window("trade_timestamp", "1 minute"),
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