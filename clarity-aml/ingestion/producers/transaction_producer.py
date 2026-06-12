"""
CLARITY AML - Synthetic Transaction Producer
=============================================
Simulates ABN AMRO's real-time transaction stream.
Generates realistic Dutch/European banking transactions
and injects hidden money laundering patterns.

Streams to Kafka topic: clarity.transactions.raw
"""

import sys
import os
import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config import config
from confluent_kafka import Producer

# ── Faker setup ───────────────────────────────────────────────
fake_nl = Faker('nl_NL')   # Dutch locale — realistic Dutch names
fake_de = Faker('de_DE')   # German locale
fake_be = Faker('fr_BE')   # Belgian locale
fake_en = Faker('en_GB')   # UK locale

# ── Real Dutch/European bank BICs ─────────────────────────────
BANK_BICS = {
    "ABN AMRO":      "ABNANL2A",
    "ING":           "INGBNL2A",
    "Rabobank":      "RABONL2U",
    "SNS Bank":      "SNSBNL2A",
    "Deutsche Bank": "DEUTDEDB",
    "Commerzbank":   "COBADEFF",
    "BNP Paribas":   "BNPAFRPP",
    "ING Belgium":   "BBRUBEBB",
    "Triodos":       "TRIONL2U",
    "Bunq":          "BUNQNL2A",
}

# ── SEPA Purpose codes (real codes used in European payments) ──
PURPOSE_CODES = {
    "SALA": "Salary payment",
    "SUPP": "Supplier payment",
    "TRAD": "Trade settlement",
    "LOAN": "Loan repayment",
    "RENT": "Rent payment",
    "UTIL": "Utility payment",
    "TAXS": "Tax payment",
    "DIVI": "Dividend payment",
    "INTC": "Intra-company transfer",
    "CASH": "Cash management",
    "GDDS": "Goods payment",
    "SVCS": "Services payment",
}

# ── Dutch company name components ─────────────────────────────
NL_COMPANY_SUFFIXES = ["BV", "NV", "VOF", "CV", "Holding BV",
                        "Group BV", "International BV", "Nederland BV"]

NL_SECTORS = ["Logistics", "Trading", "Import Export", "Consultancy",
              "Technology", "Finance", "Real Estate", "Construction",
              "Retail", "Healthcare", "Media", "Transport"]

def generate_dutch_company():
    """Generate a realistic Dutch company name."""
    name = fake_nl.last_name()
    sector = random.choice(NL_SECTORS)
    suffix = random.choice(NL_COMPANY_SUFFIXES)
    return f"{name} {sector} {suffix}"

def generate_dutch_iban():
    """Generate a realistic Dutch IBAN."""
    bank_codes = ["ABNA", "INGB", "RABO", "SNSB", "TRIO", "BUNQ"]
    bank = random.choice(bank_codes)
    account = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    check = random.randint(10, 99)
    return f"NL{check}{bank}{account}"

def generate_german_iban():
    """Generate a realistic German IBAN."""
    bank = random.randint(10000000, 99999999)
    account = random.randint(1000000000, 9999999999)
    return f"DE{random.randint(10,99)}{bank}{account}"

def generate_belgian_iban():
    """Generate a realistic Belgian IBAN."""
    return f"BE{random.randint(10,99)}{random.randint(100,999)}{random.randint(1000000,9999999)}{random.randint(10,99)}"

IBAN_GENERATORS = [
    generate_dutch_iban,
    generate_dutch_iban,    # Dutch IBANs appear more often (ABN AMRO is Dutch)
    generate_dutch_iban,
    generate_german_iban,
    generate_belgian_iban,
]

def generate_iban():
    return random.choice(IBAN_GENERATORS)()


# ══════════════════════════════════════════════════════════════
# AML PATTERN INJECTION
# These are the hidden criminal patterns your detection
# pipeline needs to find. Each pattern simulates a real
# money laundering typology used by criminals.
# ══════════════════════════════════════════════════════════════

class AMLPatternInjector:
    """
    Injects realistic money laundering patterns into the
    transaction stream. These patterns are invisible when
    looking at individual transactions but visible when
    analyzing the network.
    """

    def __init__(self):
        # Generate a fixed set of "criminal" accounts
        # These accounts will appear repeatedly in patterns
        self.structuring_accounts = self._create_criminal_network(
            size=5,
            name_prefix="Structuring"
        )
        self.layering_network = self._create_criminal_network(
            size=8,
            name_prefix="Layering"
        )
        self.circular_network = self._create_criminal_network(
            size=6,
            name_prefix="Circular"
        )

        # Track pattern state
        self.structuring_counter = 0
        self.layering_index = 0
        self.circular_index = 0

    def _create_criminal_network(self, size, name_prefix):
        """Create a fixed group of connected accounts."""
        return [{
            "iban":    generate_dutch_iban(),
            "name":    f"{name_prefix} {generate_dutch_company()}",
            "bic":     "ABNANL2A",
        } for _ in range(size)]

    def get_structuring_transaction(self):
        """
        STRUCTURING (Smurfing):
        Criminal has €100,000 to clean.
        Instead of one €100,000 deposit (which triggers a
        mandatory report above €10,000 in Netherlands),
        they split it into 11 deposits of €9,100 each.
        Each deposit looks normal. Together they're criminal.
        """
        account = random.choice(self.structuring_accounts)
        # Always just below €10,000 reporting threshold
        amount = round(random.uniform(8500, 9800), 2)
        self.structuring_counter += 1

        return {
            "sender_iban":    generate_dutch_iban(),
            "sender_name":    generate_dutch_company(),
            "sender_bic":     "ABNANL2A",
            "receiver_iban":  account["iban"],
            "receiver_name":  account["name"],
            "receiver_bic":   account["bic"],
            "amount_eur":     amount,
            "purpose_code":   "CASH",
            "aml_pattern":    "STRUCTURING",  # Tagged for validation
        }

    def get_layering_transaction(self):
        """
        LAYERING:
        Money moves through a chain of accounts to obscure
        its origin. A → B → C → D → E → F → G → H
        Each hop makes it harder to trace back to the source.
        """
        # Move to next account in the chain
        sender_idx = self.layering_index % len(self.layering_network)
        receiver_idx = (self.layering_index + 1) % len(self.layering_network)
        self.layering_index += 1

        sender   = self.layering_network[sender_idx]
        receiver = self.layering_network[receiver_idx]

        # Amount slightly decreases at each hop (fees/skimming)
        amount = round(random.uniform(15000, 45000), 2)

        return {
            "sender_iban":    sender["iban"],
            "sender_name":    sender["name"],
            "sender_bic":     sender["bic"],
            "receiver_iban":  receiver["iban"],
            "receiver_name":  receiver["name"],
            "receiver_bic":   receiver["bic"],
            "amount_eur":     amount,
            "purpose_code":   "INTC",
            "aml_pattern":    "LAYERING",
        }

    def get_circular_transaction(self):
        """
        CIRCULAR / ROUND-TRIPPING:
        Money goes A → B → C → D → A
        It comes back to where it started, now appearing
        as legitimate business income.
        """
        sender_idx   = self.circular_index % len(self.circular_network)
        receiver_idx = (self.circular_index + 1) % len(self.circular_network)
        self.circular_index += 1

        sender   = self.circular_network[sender_idx]
        receiver = self.circular_network[receiver_idx]

        return {
            "sender_iban":    sender["iban"],
            "sender_name":    sender["name"],
            "sender_bic":     sender["bic"],
            "receiver_iban":  receiver["iban"],
            "receiver_name":  receiver["name"],
            "receiver_bic":   receiver["bic"],
            "amount_eur":     round(random.uniform(20000, 80000), 2),
            "purpose_code":   "TRAD",
            "aml_pattern":    "CIRCULAR",
        }


# ══════════════════════════════════════════════════════════════
# NORMAL TRANSACTION GENERATOR
# ══════════════════════════════════════════════════════════════

def generate_normal_transaction():
    """
    Generate a completely normal, legitimate transaction.
    This is what 95% of real bank transactions look like.
    """
    purpose_code = random.choice(list(PURPOSE_CODES.keys()))

    # Amount distribution — realistic for European banking
    amount_type = random.choices(
        ["small", "medium", "large", "corporate"],
        weights=[50, 30, 15, 5]
    )[0]

    if amount_type == "small":
        amount = round(random.uniform(10, 500), 2)
    elif amount_type == "medium":
        amount = round(random.uniform(500, 5000), 2)
    elif amount_type == "large":
        amount = round(random.uniform(5000, 50000), 2)
    else:
        amount = round(random.uniform(50000, 500000), 2)

    sender_bank   = random.choice(list(BANK_BICS.items()))
    receiver_bank = random.choice(list(BANK_BICS.items()))

    return {
        "sender_iban":    generate_iban(),
        "sender_name":    generate_dutch_company(),
        "sender_bic":     sender_bank[1],
        "receiver_iban":  generate_iban(),
        "receiver_name":  generate_dutch_company(),
        "receiver_bic":   receiver_bank[1],
        "amount_eur":     amount,
        "purpose_code":   purpose_code,
        "aml_pattern":    "NONE",
    }


# ══════════════════════════════════════════════════════════════
# KAFKA PRODUCER
# ══════════════════════════════════════════════════════════════

def build_kafka_message(transaction_data):
    """
    Wraps transaction data in a full message envelope.
    This is the complete message that lands in Kafka.
    """
    return {
        # Unique ID for this transaction
        "transaction_id":   str(uuid.uuid4()),

        # Payment parties
        "sender_iban":      transaction_data["sender_iban"],
        "sender_name":      transaction_data["sender_name"],
        "sender_bic":       transaction_data["sender_bic"],
        "receiver_iban":    transaction_data["receiver_iban"],
        "receiver_name":    transaction_data["receiver_name"],
        "receiver_bic":     transaction_data["receiver_bic"],

        # Amount
        "amount_eur":       transaction_data["amount_eur"],
        "currency":         "EUR",

        # Payment details
        "purpose_code":     transaction_data["purpose_code"],
        "purpose_desc":     PURPOSE_CODES.get(
                                transaction_data["purpose_code"], "Other"
                            ),

        # Timestamps
        "value_date":       datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "booking_date":     datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "ingestion_ts":     int(datetime.now(timezone.utc).timestamp() * 1000),

        # Source system (in real ABN AMRO this would be SWIFT/SEPA system name)
        "source_system":    "SEPA_CORE",
        "message_type":     "SEPA_CT",   # SEPA Credit Transfer

        # AML label — in real life this field doesn't exist
        # We add it so we can validate our detection later
        "aml_pattern":      transaction_data["aml_pattern"],
    }


def delivery_report(err, msg):
    """Called by Kafka when a message is delivered or fails."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")


def run_producer():
    """
    Main producer loop.
    Runs forever, streaming transactions into Kafka.
    """
    print("=" * 60)
    print("  CLARITY AML — Transaction Producer")
    print("  Simulating ABN AMRO live transaction stream")
    print("=" * 60)

    # ── Kafka producer config ──────────────────────────────────
    producer_config = {
        "bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"],
        # Batch messages for efficiency (like prod)
        "batch.size":        16384,
        # Wait up to 10ms to fill a batch before sending
        "linger.ms":         10,
        # Compress messages (like prod)
        "compression.type":  "snappy",
        # Retry failed sends (like prod)
        "retries":           3,
    }

    producer  = Producer(producer_config)
    injector  = AMLPatternInjector()
    topic     = config["TOPIC_TRANSACTIONS_RAW"]

    # ── Counters ───────────────────────────────────────────────
    total_sent        = 0
    normal_sent       = 0
    structuring_sent  = 0
    layering_sent     = 0
    circular_sent     = 0
    start_time        = time.time()

    print(f"\n📡 Streaming to topic: {topic}")
    print(f"🏦 Bootstrap server:   {config['KAFKA_BOOTSTRAP_SERVERS']}")
    print(f"\n{'─' * 60}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            # ── Decide what type of transaction to generate ────
            # 92% normal, 8% AML patterns
            # In real banking, AML cases are roughly 1-5%
            # We use 8% so your detection pipeline has enough
            # signal to work with during development
            transaction_type = random.choices(
                ["normal", "structuring", "layering", "circular"],
                weights=[92, 4, 2, 2]
            )[0]

            if transaction_type == "normal":
                data = generate_normal_transaction()
                normal_sent += 1
            elif transaction_type == "structuring":
                data = injector.get_structuring_transaction()
                structuring_sent += 1
            elif transaction_type == "layering":
                data = injector.get_layering_transaction()
                layering_sent += 1
            else:
                data = injector.get_circular_transaction()
                circular_sent += 1

            # ── Build and send message ─────────────────────────
            message = build_kafka_message(data)

            producer.produce(
                topic=topic,
                key=message["transaction_id"],
                value=json.dumps(message).encode("utf-8"),
                on_delivery=delivery_report,
            )

            # Flush every 100 messages
            producer.poll(0)
            total_sent += 1

            if total_sent % 100 == 0:
                producer.flush()

            # ── Print live stats every 10 transactions ─────────
            if total_sent % 10 == 0:
                elapsed   = time.time() - start_time
                rate      = total_sent / elapsed

                print(
                    f"📊 Sent: {total_sent:,} | "
                    f"Rate: {rate:.1f}/sec | "
                    f"Normal: {normal_sent:,} | "
                    f"🚨 AML: {structuring_sent + layering_sent + circular_sent} "
                    f"[S:{structuring_sent} L:{layering_sent} C:{circular_sent}]"
                )

            # ── Speed control ──────────────────────────────────
            # 1 transaction per second = realistic for dev
            # Remove this sleep for stress testing
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n\n{'─' * 60}")
        print(f"⛔ Producer stopped by user")
        print(f"{'─' * 60}")
        print(f"📈 Final Stats:")
        print(f"   Total sent:    {total_sent:,}")
        print(f"   Normal:        {normal_sent:,}")
        print(f"   Structuring:   {structuring_sent:,}")
        print(f"   Layering:      {layering_sent:,}")
        print(f"   Circular:      {circular_sent:,}")
        elapsed = time.time() - start_time
        print(f"   Runtime:       {elapsed:.0f} seconds")
        print(f"{'─' * 60}")
        producer.flush()


if __name__ == "__main__":
    run_producer()