"""
CLARITY AML - Synthetic Transaction Producer
=============================================
Simulates ABN AMRO's real-time transaction stream.

MODES:
  python transaction_producer.py            → today only (live)
  python transaction_producer.py --backfill → 30 days history + today
  python transaction_producer.py --test     → 1500 msgs covering all edge cases
"""

import sys
import os
import json
import time
import random
import uuid
import argparse
from datetime import datetime, timezone, timedelta
from reference_loader import ReferenceLoader
from faker import Faker

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config import config
from confluent_kafka import Producer

fake_nl = Faker('nl_NL')
fake_de = Faker('de_DE')
fake_be = Faker('fr_BE')

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

NL_COMPANY_SUFFIXES = ["BV", "NV", "VOF", "CV", "Holding BV",
                        "Group BV", "International BV", "Nederland BV"]
NL_SECTORS = ["Logistics", "Trading", "Import Export", "Consultancy",
              "Technology", "Finance", "Real Estate", "Construction",
              "Retail", "Healthcare", "Media", "Transport"]

def generate_dutch_company():
    name = fake_nl.last_name()
    sector = random.choice(NL_SECTORS)
    suffix = random.choice(NL_COMPANY_SUFFIXES)
    return f"{name} {sector} {suffix}"

def generate_dutch_iban():
    bank_codes = ["ABNA", "INGB", "RABO", "SNSB", "TRIO", "BUNQ"]
    bank = random.choice(bank_codes)
    account = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    check = random.randint(10, 99)
    return f"NL{check}{bank}{account}"

def generate_german_iban():
    bank = random.randint(10000000, 99999999)
    account = random.randint(1000000000, 9999999999)
    return f"DE{random.randint(10,99)}{bank}{account}"

def generate_belgian_iban():
    return (f"BE{random.randint(10,99)}"
            f"{random.randint(100,999)}"
            f"{random.randint(1000000,9999999)}"
            f"{random.randint(10,99)}")

IBAN_GENERATORS = [
    generate_dutch_iban, generate_dutch_iban,
    generate_dutch_iban, generate_german_iban,
    generate_belgian_iban,
]

def generate_iban():
    return random.choice(IBAN_GENERATORS)()


# ══════════════════════════════════════════════════════════════
# FIXED CRIMINAL NETWORKS
# ══════════════════════════════════════════════════════════════
# CRITICAL: These IBANs are HARDCODED and never change.
# This means every run of the producer uses the same accounts.
# History accumulates across runs — layering chains build up,
# fan-in totals grow, z-score baselines establish themselves.
# Without fixed IBANs, every run is isolated and no pattern
# can span multiple days.

FIXED_NETWORKS = {
    # ── Structuring network ────────────────────────────────────
    # These accounts receive many near-threshold payments
    # 5 receiver accounts — each gets 3+ payments per run
    "structuring": [
        {"iban": "NL11ABNA0111111111", "name": "STRUCTURING ACCOUNT 1 BV",   "bic": "ABNANL2A"},
        {"iban": "NL22INGB0222222222", "name": "STRUCTURING ACCOUNT 2 BV",   "bic": "INGBNL2A"},
        {"iban": "NL33RABO0333333333", "name": "STRUCTURING ACCOUNT 3 BV",   "bic": "RABONL2U"},
        {"iban": "NL44SNSB0444444444", "name": "STRUCTURING ACCOUNT 4 BV",   "bic": "SNSBNL2A"},
        {"iban": "NL55TRIO0555555555", "name": "STRUCTURING ACCOUNT 5 BV",   "bic": "TRIONL2U"},
    ],
    # ── Layering chain ─────────────────────────────────────────
    # Money flows A→B→C→D→E→F→G→H in sequence
    # degree=2 pattern builds up over multiple runs
    "layering": [
        {"iban": "NL10ABNA1010101010", "name": "LAYERING NODE A BV",  "bic": "ABNANL2A"},
        {"iban": "NL20INGB2020202020", "name": "LAYERING NODE B BV",  "bic": "INGBNL2A"},
        {"iban": "NL30RABO3030303030", "name": "LAYERING NODE C BV",  "bic": "RABONL2U"},
        {"iban": "NL40SNSB4040404040", "name": "LAYERING NODE D BV",  "bic": "SNSBNL2A"},
        {"iban": "NL50TRIO5050505050", "name": "LAYERING NODE E BV",  "bic": "TRIONL2U"},
        {"iban": "NL60BUNQ6060606060", "name": "LAYERING NODE F BV",  "bic": "BUNQNL2A"},
        {"iban": "NL70ABNA7070707070", "name": "LAYERING NODE G BV",  "bic": "ABNANL2A"},
        {"iban": "NL80INGB8080808080", "name": "LAYERING NODE H BV",  "bic": "INGBNL2A"},
    ],
    # ── Circular network ──────────────────────────────────────
    # A→B→C→D→E→F→A — full cycle across 6 days
    # Each day one hop fires — closes on day 6
    "circular": [
        {"iban": "NL91RABO9191919191", "name": "CIRCULAR NODE A BV",  "bic": "RABONL2U"},
        {"iban": "NL82SNSB8282828282", "name": "CIRCULAR NODE B BV",  "bic": "SNSBNL2A"},
        {"iban": "NL73TRIO7373737373", "name": "CIRCULAR NODE C BV",  "bic": "TRIONL2U"},
        {"iban": "NL64BUNQ6464646464", "name": "CIRCULAR NODE D BV",  "bic": "BUNQNL2A"},
        {"iban": "NL55ABNA5555555555", "name": "CIRCULAR NODE E BV",  "bic": "ABNANL2A"},
        {"iban": "NL46INGB4646464646", "name": "CIRCULAR NODE F BV",  "bic": "INGBNL2A"},
    ],
    # ── Fan-in accumulation ────────────────────────────────────
    # One target account receives from many unique senders
    # small amounts, and immediately moves money out
    "fan_in_target": [
        {"iban": "NL99RABO9999999999", "name": "FAN IN TARGET BV",    "bic": "RABONL2U"},
    ],
    # ── Z-score baseline accounts ─────────────────────────────
    # These accounts need 10+ transactions in history
    # then on the test day receive a huge spike
    "zscore_baseline": [
        {"iban": "NL37ABNA3737373737", "name": "ZSCORE BASELINE A BV", "bic": "ABNANL2A"},
        {"iban": "NL48INGB4848484848", "name": "ZSCORE BASELINE B BV", "bic": "INGBNL2A"},
    ],
    # ── New company rapid movement ─────────────────────────────
    # Fresh accounts (no history) receiving large amounts
    # from multiple senders and immediately sending out
    "new_company": [
        {"iban": "NL15SNSB1515151515", "name": "NEW COMPANY X BV",    "bic": "SNSBNL2A"},
        {"iban": "NL26TRIO2626262626", "name": "NEW COMPANY Y BV",     "bic": "TRIONL2U"},
    ],
    # ── Cash business inflated revenue ────────────────────────
    # KvK-registered cafe with employee_count=2
    # deposits far above physical capacity
    "cash_business": [
        {"iban": "NL19ABNA1919191919", "name": "DE KLEINE CAFE BV",   "bic": "ABNANL2A"},
    ],
}

# Senders for fan-in — 10 unique senders sending to one target
FAN_IN_SENDERS = [
    {"iban": f"NL{str(i).zfill(2)}RABO{str(i)*10}", "name": f"MULE SENDER {i} BV", "bic": "RABONL2U"}
    for i in range(10, 20)
]


class AMLPatternInjector:
    def __init__(self, refs=None):
        self.refs = refs
        self.layering_index  = 0
        self.circular_index  = 0
        self.fan_in_sender_index = 0

    def get_structuring_transaction(self, days_ago=0):
        """Near-threshold payment to fixed structuring account."""
        account = random.choice(FIXED_NETWORKS["structuring"])
        amount  = round(random.uniform(7200, 9800), 2)
        return {
            "sender_iban":   generate_dutch_iban(),
            "sender_name":   generate_dutch_company(),
            "sender_bic":    "ABNANL2A",
            "receiver_iban": account["iban"],
            "receiver_name": account["name"],
            "receiver_bic":  account["bic"],
            "amount_eur":    amount,
            "purpose_code":  "CASH",
            "aml_pattern":   "STRUCTURING",
            "days_ago":      days_ago,
        }

    def get_layering_transaction(self, days_ago=0):
        """
        Sequential hop through fixed layering chain.
        A→B→C→D→E→F→G→H using always same IBANs.
        Purpose code INTC = intra-company = suspicious for unrelated parties.
        """
        chain = FIXED_NETWORKS["layering"]
        sender_idx   = self.layering_index % len(chain)
        receiver_idx = (self.layering_index + 1) % len(chain)
        self.layering_index += 1
        sender   = chain[sender_idx]
        receiver = chain[receiver_idx]
        return {
            "sender_iban":   sender["iban"],
            "sender_name":   sender["name"],
            "sender_bic":    sender["bic"],
            "receiver_iban": receiver["iban"],
            "receiver_name": receiver["name"],
            "receiver_bic":  receiver["bic"],
            "amount_eur":    round(random.uniform(15000, 45000), 2),
            "purpose_code":  "INTC",
            "aml_pattern":   "LAYERING",
            "days_ago":      days_ago,
        }

    def get_circular_transaction(self, hop=None, days_ago=0):
        """
        One hop of the circular cycle per call.
        Full cycle: A→B→C→D→E→F→A across 6 days.
        If hop is specified, uses that exact hop.
        """
        chain = FIXED_NETWORKS["circular"]
        if hop is not None:
            sender_idx   = hop % len(chain)
            receiver_idx = (hop + 1) % len(chain)
        else:
            sender_idx   = self.circular_index % len(chain)
            receiver_idx = (self.circular_index + 1) % len(chain)
            self.circular_index += 1
        sender   = chain[sender_idx]
        receiver = chain[receiver_idx]
        return {
            "sender_iban":   sender["iban"],
            "sender_name":   sender["name"],
            "sender_bic":    sender["bic"],
            "receiver_iban": receiver["iban"],
            "receiver_name": receiver["name"],
            "receiver_bic":  receiver["bic"],
            "amount_eur":    round(random.uniform(20000, 80000), 2),
            "purpose_code":  "TRAD",
            "aml_pattern":   "CIRCULAR",
            "days_ago":      days_ago,
        }

    def get_fan_in_transaction(self, days_ago=0):
        """
        Unique sender sends small amount to fixed target.
        Rotates through 10 fixed senders.
        Target then sends most of it out immediately.
        """
        sender = FAN_IN_SENDERS[self.fan_in_sender_index % len(FAN_IN_SENDERS)]
        target = FIXED_NETWORKS["fan_in_target"][0]
        self.fan_in_sender_index += 1
        return {
            "sender_iban":   sender["iban"],
            "sender_name":   sender["name"],
            "sender_bic":    sender["bic"],
            "receiver_iban": target["iban"],
            "receiver_name": target["name"],
            "receiver_bic":  target["bic"],
            "amount_eur":    round(random.uniform(500, 3000), 2),
            "purpose_code":  "CASH",
            "aml_pattern":   "FAN_IN",
            "days_ago":      days_ago,
        }

    def get_fan_in_outflow(self, days_ago=0):
        """
        Fan-in target sends 85% of what it received to one account.
        This is the rapid outflow that makes fan-in suspicious.
        """
        target = FIXED_NETWORKS["fan_in_target"][0]
        return {
            "sender_iban":   target["iban"],
            "sender_name":   target["name"],
            "sender_bic":    target["bic"],
            "receiver_iban": generate_dutch_iban(),
            "receiver_name": generate_dutch_company(),
            "receiver_bic":  "ABNANL2A",
            "amount_eur":    round(random.uniform(8000, 15000), 2),
            "purpose_code":  "INTC",
            "aml_pattern":   "FAN_IN",
            "days_ago":      days_ago,
        }

    def get_zscore_baseline_transaction(self, days_ago=0):
        """
        Normal small transaction from baseline account.
        Builds historical mean of ~€400.
        On test day we send €50,000 — z-score will be huge.
        """
        account = random.choice(FIXED_NETWORKS["zscore_baseline"])
        return {
            "sender_iban":   account["iban"],
            "sender_name":   account["name"],
            "sender_bic":    account["bic"],
            "receiver_iban": generate_dutch_iban(),
            "receiver_name": generate_dutch_company(),
            "receiver_bic":  "ABNANL2A",
            "amount_eur":    round(random.uniform(200, 800), 2),
            "purpose_code":  "SUPP",
            "aml_pattern":   "NONE",
            "days_ago":      days_ago,
        }

    def get_zscore_spike_transaction(self):
        """
        Today: baseline account suddenly sends €50,000+.
        Historical mean was ~€400. z-score = (50000-400)/std ≈ huge.
        """
        account = random.choice(FIXED_NETWORKS["zscore_baseline"])
        return {
            "sender_iban":   account["iban"],
            "sender_name":   account["name"],
            "sender_bic":    account["bic"],
            "receiver_iban": generate_dutch_iban(),
            "receiver_name": generate_dutch_company(),
            "receiver_bic":  "ABNANL2A",
            "amount_eur":    round(random.uniform(45000, 80000), 2),
            "purpose_code":  "CASH",
            "aml_pattern":   "ZSCORE_SPIKE",
            "days_ago":      0,
        }

    def get_cash_business_transaction(self, days_ago=0):
        """
        Cafe with 2 employees deposits €8,000/day.
        KvK capacity: 2 × €150 × 2 = €600 max.
        €8,000 >> €600 → cash intensity anomaly.
        """
        account = FIXED_NETWORKS["cash_business"][0]
        return {
            "sender_iban":   generate_dutch_iban(),
            "sender_name":   generate_dutch_company(),
            "sender_bic":    "ABNANL2A",
            "receiver_iban": account["iban"],
            "receiver_name": account["name"],
            "receiver_bic":  account["bic"],
            "amount_eur":    round(random.uniform(6500, 9200), 2),
            "purpose_code":  "CASH",
            "aml_pattern":   "CASH_BUSINESS",
            "days_ago":      days_ago,
        }

    def get_new_company_transaction(self, days_ago=0):
        """
        Brand new account receives large amounts from
        multiple unrelated senders immediately.
        """
        account = random.choice(FIXED_NETWORKS["new_company"])
        return {
            "sender_iban":   generate_dutch_iban(),
            "sender_name":   generate_dutch_company(),
            "sender_bic":    "ABNANL2A",
            "receiver_iban": account["iban"],
            "receiver_name": account["name"],
            "receiver_bic":  account["bic"],
            "amount_eur":    round(random.uniform(20000, 150000), 2),
            "purpose_code":  "TRAD",
            "aml_pattern":   "NEW_COMPANY_RAPID_MOVEMENT",
            "days_ago":      days_ago,
        }


def generate_normal_transaction(refs=None, days_ago=0):
    purpose_code = random.choice(list(PURPOSE_CODES.keys()))
    amount_type  = random.choices(
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

    name_type = random.choices(
        ["kvk", "sanctioned", "random"],
        weights=[50, 10, 40]
    )[0]
    if name_type == "kvk" and refs:
        sender_name = refs.get_kvk_company_name()
    elif name_type == "sanctioned" and refs:
        sender_name = refs.get_sanctioned_name()
    else:
        sender_name = generate_dutch_company()

    receiver_name = (refs.get_kvk_company_name()
                     if random.random() < 0.5 and refs
                     else generate_dutch_company())

    return {
        "sender_iban":   generate_iban(),
        "sender_name":   sender_name,
        "sender_bic":    sender_bank[1],
        "receiver_iban": generate_iban(),
        "receiver_name": receiver_name,
        "receiver_bic":  receiver_bank[1],
        "amount_eur":    amount,
        "purpose_code":  purpose_code,
        "aml_pattern":   "NONE",
        "days_ago":      days_ago,
    }


def build_kafka_message(transaction_data):
    """
    Build Kafka message with backdated timestamp.
    days_ago=0 → today
    days_ago=5 → 5 days ago
    This is what allows historical data to exist in Silver
    when notebooks read it — the partition filter uses value_date.
    """
    days_ago = transaction_data.pop("days_ago", 0)
    txn_time = datetime.now(timezone.utc) - timedelta(days=days_ago)

    # Randomise hour within business day for realism
    # Historical transactions: random business hour
    # Today's transactions: actual current hour
    if days_ago > 0:
        hour   = random.randint(8, 17)
        minute = random.randint(0, 59)
        txn_time = txn_time.replace(hour=hour, minute=minute,
                                     second=0, microsecond=0)

    return {
        "transaction_id":   str(uuid.uuid4()),
        "sender_iban":      transaction_data["sender_iban"],
        "sender_name":      transaction_data["sender_name"],
        "sender_bic":       transaction_data["sender_bic"],
        "receiver_iban":    transaction_data["receiver_iban"],
        "receiver_name":    transaction_data["receiver_name"],
        "receiver_bic":     transaction_data["receiver_bic"],
        "amount_eur":       transaction_data["amount_eur"],
        "currency":         "EUR",
        "purpose_code":     transaction_data["purpose_code"],
        "purpose_desc":     PURPOSE_CODES.get(
                                transaction_data["purpose_code"], "Other"
                            ),
        "value_date":       txn_time.strftime("%Y-%m-%d"),
        "booking_date":     txn_time.strftime("%Y-%m-%d"),
        "ingestion_ts":     int(txn_time.timestamp() * 1000),
        "transaction_hour": txn_time.hour,
        "day_of_week":      txn_time.weekday(),
        "source_system":    "SEPA_CORE",
        "message_type":     "SEPA_CT",
        "aml_pattern":      transaction_data["aml_pattern"],
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")


def build_test_batch(refs, injector):
    """
    Build exactly 1,500 transactions covering every edge case.
    Returns list of (message_dict) in send order.

    Distribution:
      700  normal transactions across 30 days  → baseline
       50  structuring transactions             → classic structuring
       80  layering transactions                → full chain multiple times
       60  fan-in inbound transactions          → 10 senders × 6 days
       20  fan-in outflow transactions          → rapid outflow signal
       60  circular hops spread across 6 days  → full cycle closes
       80  z-score baseline (small, historical) → builds mean
        5  z-score spikes (today)               → z > 3 fires
       30  cash business deposits               → capacity anomaly
       20  new company transactions (today)     → coherence check
      395  additional normal padding            → realistic ratio
    Total: ~1,500
    """
    messages = []

    # ── 700 normal transactions spread across 30 days ──────────
    for _ in range(700):
        days_ago = random.randint(1, 29)
        data = generate_normal_transaction(refs, days_ago=days_ago)
        messages.append(build_kafka_message(data))

    # ── 50 structuring: 3+ near-threshold to same account daily ─
    # Spread across 10 days so receiver builds up count in history
    for day in range(10):
        for _ in range(5):
            data = injector.get_structuring_transaction(days_ago=day)
            messages.append(build_kafka_message(data))

    # ── 80 layering: full chain A→B→C→D→E→F→G→H ──────────────
    # Run the full chain 10 times across 14 days
    # Each chain cycle = 8 hops. 10 cycles × 8 = 80.
    for cycle in range(10):
        days_ago = 14 - cycle  # spread over 14 days
        for hop in range(8):
            injector.layering_index = hop  # force hop position
            data = injector.get_layering_transaction(days_ago=days_ago)
            messages.append(build_kafka_message(data))

    # ── 60 fan-in inbound: 10 unique senders × 6 days ─────────
    # Each sender sends twice per day for 3 days
    # Total received: 10 senders × ~€1,500 avg × 6 = ~€90,000
    # avg_inbound_amount = ~€1,500 < €5,000 ✓
    # unique_senders_total = 10 >= 5 ✓
    # total_received_eur = ~€90,000 >= €50,000 ✓
    for day in range(6):
        injector.fan_in_sender_index = 0
        for _ in range(10):
            data = injector.get_fan_in_transaction(days_ago=5 - day)
            messages.append(build_kafka_message(data))

    # ── 20 fan-in outflow: target sends most money out ─────────
    # This fires s1_low_retention and s2_concentrated_outbound
    for day in range(5):
        for _ in range(4):
            data = injector.get_fan_in_outflow(days_ago=5 - day)
            messages.append(build_kafka_message(data))

    # ── 60 circular hops spread across 6 days ─────────────────
    # Cycle: A→B→C→D→E→F→A
    # 10 complete cycles × 6 hops = 60 transactions
    # Spread over 6 days so rolling window sees full cycle
    chain_len = len(FIXED_NETWORKS["circular"])
    for cycle in range(10):
        for hop in range(chain_len):
            # Day 5 ago = hop 0, day 4 ago = hop 1, etc.
            days_ago = (chain_len - 1) - (hop % chain_len)
            data = injector.get_circular_transaction(
                hop=hop, days_ago=days_ago
            )
            messages.append(build_kafka_message(data))

    # ── 80 z-score baseline: small historical transactions ──────
    # Each baseline account needs 10+ transactions
    # 2 accounts × 40 historical transactions = 80
    for _ in range(80):
        days_ago = random.randint(5, 29)
        data = injector.get_zscore_baseline_transaction(days_ago=days_ago)
        messages.append(build_kafka_message(data))

    # ── 5 z-score spikes: today, huge amount ──────────────────
    for _ in range(5):
        data = injector.get_zscore_spike_transaction()
        messages.append(build_kafka_message(data))

    # ── 30 cash business deposits ─────────────────────────────
    # Same cafe deposits €8,000/day for 10 days
    for day in range(10):
        for _ in range(3):
            data = injector.get_cash_business_transaction(days_ago=day)
            messages.append(build_kafka_message(data))

    # ── 20 new company transactions (today only) ───────────────
    # New account receives from many different senders today
    for _ in range(20):
        data = injector.get_new_company_transaction(days_ago=0)
        messages.append(build_kafka_message(data))

    # ── 395 additional normal padding ─────────────────────────
    # Ensures realistic AML ratio — not 100% suspicious
    for _ in range(395):
        days_ago = random.randint(0, 29)
        data = generate_normal_transaction(refs, days_ago=days_ago)
        messages.append(build_kafka_message(data))

    # Shuffle so historical and today's transactions are interleaved
    # This simulates realistic Kafka consumption order
    random.shuffle(messages)

    return messages


def run_test_mode(producer, refs, injector, topic):
    """
    Sends exactly ~1,500 transactions covering all edge cases.
    No sleep between messages — sends as fast as Kafka allows.
    All transactions are pre-built then sent in one burst.
    """
    print("\n" + "═" * 60)
    print("  TEST MODE — Building 1,500 test transactions")
    print("  Covers: structuring, layering, circular,")
    print("  fan-in, z-score, cash business, new company")
    print("═" * 60)

    print("\n📦 Building test batch...")
    messages = build_test_batch(refs, injector)
    total = len(messages)
    print(f"✅ Built {total:,} messages")

    # Count by pattern
    from collections import Counter
    pattern_counts = Counter(m["aml_pattern"] for m in messages)
    print("\n📊 Pattern distribution:")
    for pattern, count in sorted(pattern_counts.items()):
        print(f"   {pattern:<30} {count:>4}")

    print(f"\n📡 Sending to topic: {topic}")
    print("   (no sleep — sending as fast as possible)\n")

    sent = 0
    start = time.time()

    for message in messages:
        producer.produce(
            topic=topic,
            key=message["transaction_id"],
            value=json.dumps(message).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            elapsed = time.time() - start
            print(f"   Sent {sent:,}/{total:,} "
                  f"({sent/elapsed:.0f} msg/sec)")

    producer.flush()
    elapsed = time.time() - start

    print(f"\n{'─' * 60}")
    print(f"✅ Test batch complete")
    print(f"   Total sent:  {sent:,}")
    print(f"   Time taken:  {elapsed:.1f}s")
    print(f"   Rate:        {sent/elapsed:.0f} msg/sec")
    print(f"{'─' * 60}")
    print("\n⏭️  Next steps:")
    print("   1. Wait for Bronze writer to flush all batches")
    print("   2. Run 01_fuzzy_entity_matching (process ALL dates)")
    print("   3. Run 02_aml_graph_detection")
    print("   You should see:")
    print("   ✅ Structuring: ~5 accounts flagged")
    print("   ✅ Layering:    chain detected, confirmed")
    print("   ✅ Circular:    A→B→C→D→E→F→A detected")
    print("   ✅ Fan-in:      1 account, 3/4 signals")
    print("   ✅ Z-score:     2 accounts flagged")
    print("   ✅ Cash biz:    capacity anomaly detected")


def run_live_mode(producer, refs, injector, topic):
    """Original live mode — one transaction per second forever."""
    print(f"\n📡 Streaming to topic: {topic}")
    print(f"🏦 Bootstrap server:   {config['KAFKA_BOOTSTRAP_SERVERS']}")
    print(f"\n{'─' * 60}")
    print("Press Ctrl+C to stop\n")

    total_sent = normal_sent = structuring_sent = 0
    layering_sent = circular_sent = new_company_sent = 0
    start_time = time.time()

    try:
        while True:
            transaction_type = random.choices(
                ["normal", "structuring", "layering",
                 "circular", "new_company"],
                weights=[88, 4, 2, 2, 2]
            )[0]

            if transaction_type == "normal":
                data = generate_normal_transaction(refs, days_ago=0)
                normal_sent += 1
            elif transaction_type == "structuring":
                data = injector.get_structuring_transaction(days_ago=0)
                structuring_sent += 1
            elif transaction_type == "layering":
                data = injector.get_layering_transaction(days_ago=0)
                layering_sent += 1
            elif transaction_type == "new_company":
                data = injector.get_new_company_transaction(days_ago=0)
                new_company_sent += 1
            else:
                data = injector.get_circular_transaction(days_ago=0)
                circular_sent += 1

            message = build_kafka_message(data)
            producer.produce(
                topic=topic,
                key=message["transaction_id"],
                value=json.dumps(message).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            total_sent += 1

            if total_sent % 100 == 0:
                producer.flush()

            if total_sent % 10 == 0:
                elapsed = time.time() - start_time
                rate    = total_sent / elapsed
                print(
                    f"📊 Sent: {total_sent:,} | Rate: {rate:.1f}/sec | "
                    f"Normal: {normal_sent:,} | "
                    f"🚨 AML: "
                    f"[S:{structuring_sent} L:{layering_sent} "
                    f"C:{circular_sent} NC:{new_company_sent}]"
                )
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n\n{'─' * 60}")
        print(f"⛔ Producer stopped")
        print(f"   Total: {total_sent:,} | Normal: {normal_sent:,} | "
              f"AML: {structuring_sent + layering_sent + circular_sent:,}")
        producer.flush()


def run_producer():
    parser = argparse.ArgumentParser(description="CLARITY AML Producer")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Send ~1500 test transactions covering all edge cases then stop"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  CLARITY AML — Transaction Producer")
    print("  Simulating ABN AMRO live transaction stream")
    if args.test:
        print("  MODE: TEST (1500 msgs, all patterns, then stop)")
    else:
        print("  MODE: LIVE (streaming, press Ctrl+C to stop)")
    print("=" * 60)

    producer_config = {
        "bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"],
        "batch.size":        16384,
        "linger.ms":         10,
        "compression.type":  "snappy",
        "retries":           3,
    }

    producer = Producer(producer_config)
    refs     = ReferenceLoader()
    injector = AMLPatternInjector(refs=refs)
    topic    = config["TOPIC_TRANSACTIONS_RAW"]

    if args.test:
        run_test_mode(producer, refs, injector, topic)
    else:
        run_live_mode(producer, refs, injector, topic)


if __name__ == "__main__":
    run_producer()