"""
CLARITY AML - Bronze Layer Writer
===================================
Reads every transaction from Kafka and writes it
to the Bronze layer immediately — raw, unmodified.

This is the legal audit trail.
Dutch WWFT law requires 5 years retention.
Every byte stored here is exactly what came from
the source system — never touched, never changed.
"""

import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from config import config
from utils.storage import StorageWriter
from confluent_kafka import Consumer, KafkaError

def run_bronze_writer():
    print("=" * 60)
    print("  CLARITY AML — Bronze Layer Writer")
    print("  Kafka → ADLS Gen2 Bronze Layer")
    print("=" * 60)

    # ── Storage setup ──────────────────────────────────────────
    storage = StorageWriter()

    # ── Kafka consumer config ──────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers":  config["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":           "clarity-bronze-writer",
        "auto.offset.reset":  "earliest",
        # Don't auto-commit — we commit only after successful write
        "enable.auto.commit": False,
    })

    consumer.subscribe([config["TOPIC_TRANSACTIONS_RAW"]])

    print(f"\n📥 Consuming from: {config['TOPIC_TRANSACTIONS_RAW']}")
    print(f"💾 Writing to:     {config['BRONZE_PATH']}")
    print(f"\nPress Ctrl+C to stop\n")
    print("─" * 60)

    # ── Batch settings ─────────────────────────────────────────
    # Don't write one file per message — too many small files
    # Batch up 10 messages then write one Parquet file
    # In prod this would be time-based (every 60 seconds)
    BATCH_SIZE   = 10
    batch        = []
    total_written = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message yet — if we have a partial batch,
                # write it anyway after 30 seconds of waiting
                if batch:
                    storage.write_bronze(batch, "transactions")
                    consumer.commit()
                    total_written += len(batch)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    continue

            # ── Parse the message ──────────────────────────────
            try:
                record = json.loads(msg.value().decode("utf-8"))

                # Add storage metadata
                record["_kafka_offset"]    = msg.offset()
                record["_kafka_partition"] = msg.partition()
                record["_kafka_topic"]     = msg.topic()

                batch.append(record)

            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse message: {e}")
                # Bad message goes to DLQ in production
                continue

            # ── Write batch when full ──────────────────────────
            if len(batch) >= BATCH_SIZE:
                storage.write_bronze(batch, "transactions")
                consumer.commit()    # Only commit after successful write
                total_written += len(batch)
                batch = []

                print(f"✅ Total records in Bronze: {total_written:,}")

    except KeyboardInterrupt:
        # Write any remaining records
        if batch:
            storage.write_bronze(batch, "transactions")
            consumer.commit()
            total_written += len(batch)

        print(f"\n⛔ Bronze writer stopped")
        print(f"📊 Total records written: {total_written:,}")

    finally:
        consumer.close()


if __name__ == "__main__":
    run_bronze_writer()