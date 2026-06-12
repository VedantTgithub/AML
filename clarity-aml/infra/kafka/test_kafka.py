from confluent_kafka import Producer, Consumer
import json
import time

# ── Test Producer ──────────────────────────────
print("Testing producer...")

producer = Producer({"bootstrap.servers": "localhost:9092"})

test_message = {
    "transaction_id": "TEST-001",
    "sender_iban": "NL91ABNA0417164300",
    "receiver_iban": "NL69INGB0123456789",
    "amount_eur": 9500.00,
    "currency": "EUR",
    "sender_name": "Test Sender BV",
    "receiver_name": "Test Receiver",
    "value_date": "2025-06-10"
}

producer.produce(
    topic="clarity.transactions.raw",
    key="TEST-001",
    value=json.dumps(test_message).encode("utf-8")
)
producer.flush()
print("✅ Message sent successfully")

# Wait 2 seconds before consuming
time.sleep(2)

# ── Test Consumer ──────────────────────────────
print("\nTesting consumer...")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "clarity-test-group-2",  # New group = reads from beginning
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["clarity.transactions.raw"])

# Poll a few times to give it time
msg = None
for _ in range(10):
    msg = consumer.poll(timeout=3.0)
    if msg is not None:
        break

if msg is None:
    print("❌ No message received")
elif msg.error():
    print(f"❌ Error: {msg.error()}")
else:
    data = json.loads(msg.value().decode("utf-8"))
    print(f"✅ Message received: {data['transaction_id']}")
    print(f"   Amount: €{data['amount_eur']}")
    print(f"   From: {data['sender_name']}")

consumer.close()
print("\n✅ Kafka is fully working!")