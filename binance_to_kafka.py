"""
Binance WebSocket -> Kafka producer.

Adapted from the Event Hub version. Same WebSocket ingestion logic
(reconnect with exponential backoff, graceful shutdown), but publishes
to a local Kafka broker instead of Azure Event Hub.

Key difference worth understanding for interviews: each event is keyed
by its trading symbol (btcusdt, ethusdt, ...). Kafka hashes the key to
decide which partition a message goes to -- messages with the SAME key
always land on the SAME partition, which guarantees per-symbol
ordering (all btcusdt trades arrive in order relative to each other,
even though ethusdt trades might interleave on a different partition).
This is the concrete mechanic behind "how partitions distribute data"
that you wanted to be able to explain.

Usage:
    python binance_to_kafka.py

Required environment variables:
    KAFKA_BOOTSTRAP_SERVERS   e.g. "localhost:9092" (local Docker Kafka)
    KAFKA_TOPIC               e.g. "trades-stream"

Optional environment variables:
    BINANCE_SYMBOLS             Comma-separated trading pairs
                                 (default: "btcusdt,ethusdt")
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("binance_kafka_producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SYMBOLS = [s.strip().lower() for s in os.environ.get("BINANCE_SYMBOLS", "btcusdt,ethusdt").split(",")]

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    f"{symbol}@trade" for symbol in SYMBOLS
)

_shutdown = asyncio.Event()


def _handle_shutdown_signal(*_args):
    logger.info("Shutdown signal received, stopping gracefully...")
    _shutdown.set()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # linger_ms: the producer waits up to this long to accumulate
        # more messages before sending a batch over the network, instead
        # of sending one message per network call -- this is Kafka's
        # built-in equivalent of the manual batching the Event Hub
        # version had to do by hand.
        linger_ms=200,
        batch_size=32_768,
        acks="all",  # wait for the broker to confirm the write, not just accept it
    )


async def stream_trades(queue: "asyncio.Queue[dict]") -> None:
    backoff_seconds = 1
    while not _shutdown.is_set():
        try:
            logger.info("Connecting to Binance WebSocket for symbols: %s", SYMBOLS)
            async with websockets.connect(BINANCE_WS_URL, ping_interval=20) as ws:
                logger.info("Connected.")
                backoff_seconds = 1

                while not _shutdown.is_set():
                    raw_message = await asyncio.wait_for(ws.recv(), timeout=30)
                    payload = json.loads(raw_message)
                    trade = payload.get("data")
                    if trade is None:
                        continue

                    event = {
                        "event_type": trade.get("e"),
                        "event_time_ms": trade.get("E"),
                        "symbol": trade.get("s"),
                        "trade_id": trade.get("t"),
                        "price": trade.get("p"),
                        "quantity": trade.get("q"),
                        "trade_time_ms": trade.get("T"),
                        "is_buyer_market_maker": trade.get("m"),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                    await queue.put(event)

        except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as exc:
            logger.warning("Connection lost (%s). Reconnecting in %ss...", exc, backoff_seconds)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)


async def publish_to_kafka(queue: "asyncio.Queue[dict]") -> None:
    producer = make_producer()
    sent_count = 0

    try:
        while not _shutdown.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # key=event["symbol"] is what determines the partition --
            # every btcusdt trade goes to the same partition as every
            # other btcusdt trade, deterministically, based on the hash
            # of this key.
            producer.send(KAFKA_TOPIC, key=event["symbol"], value=event)
            sent_count += 1

            if sent_count % 100 == 0:
                logger.info("Sent %d events so far", sent_count)
                producer.flush()  # periodic flush so counts/logs stay honest
    finally:
        producer.flush()
        producer.close()


async def main() -> None:
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_shutdown_signal)
        except NotImplementedError:
            pass  # Windows default event loop: Ctrl+C still raises KeyboardInterrupt

    queue: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=10_000)

    await asyncio.gather(
        stream_trades(queue),
        publish_to_kafka(queue),
    )


if __name__ == "__main__":
    asyncio.run(main())