# infra/kafka/create_topics.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from config import config
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    (config["TOPIC_TRANSACTIONS_RAW"],    6, 1),
    (config["TOPIC_ENTITIES_RESOLVED"],   3, 1),
    (config["TOPIC_ALERTS_GENERATED"],    3, 1),
    ("clarity.graph.updates",             3, 1),
    (config["TOPIC_REPORTS_FIU"],         1, 1),
    (config["TOPIC_DLQ_TRANSACTIONS"],    1, 1),
    (config["TOPIC_SANCTIONS_UPDATES"],   1, 1),
]

def create_topics():
    admin = AdminClient({
        "bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"]
    })

    new_topics = [
        NewTopic(name, num_partitions=p, replication_factor=r)
        for name, p, r in TOPICS
    ]

    result = admin.create_topics(new_topics)

    for topic, future in result.items():
        try:
            future.result()
            print(f"✅ Topic created: {topic}")
        except Exception as e:
            print(f"⚠️  {topic}: {e}")

if __name__ == "__main__":
    create_topics()