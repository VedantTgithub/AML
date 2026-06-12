import os
from dotenv import load_dotenv
from pathlib import Path

def load_config():
    env = os.getenv("ENV", "dev")
    env_file = Path(__file__).parent / f".env.{env}"
    
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded config from {env_file}")
    else:
        print(f"⚠️  No .env.{env} file found, using system environment")

    return {
        # Kafka
        "KAFKA_BOOTSTRAP_SERVERS":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "KAFKA_SECURITY_PROTOCOL":  os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),

        # Topics — all have fallback defaults so missing .env entries never crash
        "TOPIC_TRANSACTIONS_RAW":   os.getenv("TOPIC_TRANSACTIONS_RAW",  "clarity.transactions.raw"),
        "TOPIC_ENTITIES_RESOLVED":  os.getenv("TOPIC_ENTITIES_RESOLVED", "clarity.entities.resolved"),
        "TOPIC_ALERTS_GENERATED":   os.getenv("TOPIC_ALERTS_GENERATED",  "clarity.alerts.generated"),
        "TOPIC_GRAPH_UPDATES":      os.getenv("TOPIC_GRAPH_UPDATES",     "clarity.graph.updates"),
        "TOPIC_REPORTS_FIU":        os.getenv("TOPIC_REPORTS_FIU",       "clarity.reports.fiu"),
        "TOPIC_DLQ_TRANSACTIONS":   os.getenv("TOPIC_DLQ_TRANSACTIONS",  "clarity.dlq.transactions"),
        "TOPIC_SANCTIONS_UPDATES":  os.getenv("TOPIC_SANCTIONS_UPDATES", "clarity.sanctions.updates"),

        # Storage
        "BRONZE_PATH":              os.getenv("BRONZE_PATH", "./data/lake/bronze"),
        "SILVER_PATH":              os.getenv("SILVER_PATH", "./data/lake/silver"),
        "GOLD_PATH":                os.getenv("GOLD_PATH",   "./data/lake/gold"),

        # Snowflake
        "SNOWFLAKE_ACCOUNT":        os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_DATABASE":       os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_WAREHOUSE":      os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_SCHEMA":         os.getenv("SNOWFLAKE_SCHEMA", "AML"),
        "SNOWFLAKE_ROLE":           os.getenv("SNOWFLAKE_ROLE"),
    }

config = load_config()