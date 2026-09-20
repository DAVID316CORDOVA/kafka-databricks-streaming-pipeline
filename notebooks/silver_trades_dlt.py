# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT - capa Silver
# MAGIC
# MAGIC Se lee la tabla Bronze (`kafka_bronze.trades_raw_kafka`), poblada por el
# MAGIC job de streaming clasico ya desplegado. No se requiere que Kafka ni el
# MAGIC productor local esten corriendo: se lee la tabla Delta que ya existe, y
# MAGIC el pipeline continua leyendo en modo streaming si Bronze vuelve a
# MAGIC recibir datos en el futuro.

import dlt
from pyspark.sql import functions as F

BRONZE_TABLE = "dbw_fintech_fdcg01.kafka_bronze.trades_raw_kafka"


# ---------------------------------------------------------------------------
# CHECKPOINTING
# ---------------------------------------------------------------------------
# A diferencia del job de Bronze (Structured Streaming manual, donde se
# declaro explicitamente un checkpointLocation dentro de un Volume), en DLT
# el checkpoint no se configura de forma manual. Cada tabla declarada con
# @dlt.table recibe su propio checkpoint interno, gestionado automaticamente
# por el motor de DLT y ligado al pipeline. Ese checkpoint registra, para
# cada micro-batch procesado, los offsets de origen leidos y el estado de
# escritura correspondiente, de forma atomica. Es lo que permite que, si el
# pipeline se detiene o falla y se vuelve a ejecutar, no se reprocesen datos
# ya escritos ni se pierdan datos a medio procesar. El progreso puede
# inspeccionarse desde la pestana "Event log" del pipeline en la UI, sin
# necesidad de acceder a ningun archivo de checkpoint manualmente.


# ---------------------------------------------------------------------------
# WATERMARKING
# ---------------------------------------------------------------------------
# Se declara un watermark de 2 minutos sobre trade_timestamp. Esto establece
# que el motor de Spark espera hasta 2 minutos por eventos que lleguen fuera
# de orden antes de considerar cerrada una ventana de tiempo. Cualquier
# evento cuyo trade_timestamp sea mas antiguo que (maximo_timestamp_visto -
# 2 minutos) se descarta de forma silenciosa, y el estado interno asociado a
# esa ventana se libera de memoria. Sin este mecanismo, el estado usado para
# deduplicar u agregar datos streaming crece de forma indefinida, y el
# cluster eventualmente se queda sin memoria disponible.

@dlt.table(
    name="trades_clean",
    comment="Trades limpios y deduplicados, con watermark de 2 minutos",
    table_properties={"quality": "silver"},
)
# expect_or_drop: cuando una fila no cumple la condicion, se descarta de
# forma silenciosa y el pipeline continua su ejecucion con normalidad.
# Se reserva para problemas de calidad de datos que no son criticos.
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
# expect_or_fail: cuando una fila no cumple la condicion, la ejecucion del
# pipeline se detiene con error. Se reserva para violaciones criticas de
# esquema, donde continuar procesando produciria datos incorrectos en las
# capas siguientes (en este caso, Gold dependeria de un symbol o timestamp
# invalido para construir sus ventanas de tiempo).
@dlt.expect_or_fail(
    "valid_symbol_and_timestamp",
    "symbol IS NOT NULL AND trade_timestamp IS NOT NULL",
)
def trades_clean():
    raw_stream = spark.readStream.table(BRONZE_TABLE)

    return (
        raw_stream.withWatermark("trade_timestamp", "2 minutes")
        # dropDuplicatesWithinWatermark compara duplicados unicamente dentro
        # de la ventana de watermark activa, por lo que no se necesita
        # mantener en memoria el historial completo de trade_id vistos.
        .dropDuplicatesWithinWatermark(["symbol", "trade_id"])
        .select(
            "symbol",
            "price",
            "quantity",
            "trade_timestamp",
            F.current_timestamp().alias("silver_processed_at"),
        )
    )