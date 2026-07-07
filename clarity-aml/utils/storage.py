"""
CLARITY AML - Storage Utility
==============================
Handles both local simulation and Azure ADLS Gen2.
The calling code never knows which one it's using.
Everything is controlled by the ENV variable.
"""

import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import config


class StorageWriter:
    """
    Single class that writes to either local filesystem
    or Azure ADLS Gen2 depending on environment.
    
    Calling code is identical regardless of environment:
    
        writer = StorageWriter()
        writer.write_bronze(records, "transactions")
    """

    def __init__(self):
        self.storage_type = os.getenv("STORAGE_TYPE", "local")
        self.bronze_path  = config["BRONZE_PATH"]
        self.silver_path  = config["SILVER_PATH"]
        self.gold_path    = config["GOLD_PATH"]

        if self.storage_type == "azure":
            self._setup_azure()
        else:
            self._setup_local()

        print(f"✅ Storage initialised: {self.storage_type.upper()}")
        print(f"   Bronze: {self.bronze_path}")

    def _setup_local(self):
        """Create local folder structure mirroring ADLS Gen2."""
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            Path(path).mkdir(parents=True, exist_ok=True)
        print("📁 Local data lake folders created")

    def _setup_azure(self):
        """Configure Azure ADLS Gen2 connection."""
        from azure.storage.filedatalake import DataLakeServiceClient
        from azure.identity import ClientSecretCredential

        credential = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )

        self.azure_client = DataLakeServiceClient(
            account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.dfs.core.windows.net",
            credential=credential
        )
        print("☁️  Azure ADLS Gen2 connected")

    def _get_partition_path(self, base_path, dataset_name):
        """
        Build a date-partitioned path.
        
        Local:  ./data/lake/bronze/transactions/year=2025/month=06/day=10/
        Azure:  abfss://bronze@....net/transactions/year=2025/month=06/day=10/
        
        Same structure, different root. Spark reads both identically.
        """
        now = datetime.now(timezone.utc)
        return os.path.join(
            base_path,
            dataset_name,
            f"year={now.strftime('%Y')}",
            f"month={now.strftime('%m')}",
            f"day={now.strftime('%d')}",
        )

    # def write_bronze(self, records: list, dataset_name: str):
    #     """
    #     Write raw records to Bronze layer.
    #     Bronze = exactly as received, never modified.
        
    #     Works identically for local and Azure.
    #     """
    #     if not records:
    #         return

    #     partition_path = self._get_partition_path(
    #         self.bronze_path, dataset_name
    #     )

    #     # Convert records to PyArrow table
    #     table = pa.Table.from_pylist(records)

    #     # Generate filename with timestamp
    #     ts = datetime.now(timezone.utc).strftime("%H_%M_%S")
    #     filename = f"{dataset_name}_{ts}.parquet"
    #     full_path = os.path.join(partition_path, filename)

    #     if self.storage_type == "local":
    #         # ── LOCAL: write directly to filesystem ───────────
    #         Path(partition_path).mkdir(parents=True, exist_ok=True)
    #         pq.write_table(table, full_path)

    #     elif self.storage_type == "azure":
    #         # ── AZURE: write to ADLS Gen2 ─────────────────────
    #         # Same parquet format, different destination
    #         import io
    #         buffer = io.BytesIO()
    #         pq.write_table(table, buffer)
    #         buffer.seek(0)

    #         # Parse container and path from abfss:// URL
    #         # abfss://bronze@account.net/transactions/...
    #         container = self.bronze_path.split("//")[1].split("@")[0]
    #         file_path = full_path.replace(
    #             f"abfss://{container}@{os.getenv('AZURE_STORAGE_ACCOUNT')}.dfs.core.windows.net/",
    #             ""
    #         )

    #         fs_client = self.azure_client.get_file_system_client(container)
    #         file_client = fs_client.get_file_client(file_path)
    #         file_client.upload_data(buffer.read(), overwrite=True)

    #     print(f"💾 Bronze written: {full_path} ({len(records)} records)")
    #     return full_path

    def write_bronze(self, records: list, dataset_name: str):
        if not records:
            return

        # ── Fixed schema enforced on every parquet file ────────────
        # Without this, pyarrow infers schema per-batch and files
        # written at different times get different column types.
        # Spark/ADF crashes when merging mismatched schemas across
        # partitions. This schema must match the Kafka message exactly.
        BRONZE_SCHEMA = pa.schema([
            pa.field("transaction_id",   pa.string()),
            pa.field("sender_iban",      pa.string()),
            pa.field("sender_name",      pa.string()),
            pa.field("sender_bic",       pa.string()),
            pa.field("receiver_iban",    pa.string()),
            pa.field("receiver_name",    pa.string()),
            pa.field("receiver_bic",     pa.string()),
            pa.field("amount_eur",       pa.float64()),
            pa.field("currency",         pa.string()),
            pa.field("purpose_code",     pa.string()),
            pa.field("purpose_desc",     pa.string()),
            pa.field("value_date",       pa.string()),
            pa.field("booking_date",     pa.string()),
            pa.field("ingestion_ts",     pa.int64()),
            pa.field("value_date_year",  pa.int64()),
            pa.field("value_date_month", pa.int64()),
            pa.field("value_date_day",   pa.int64()),
            pa.field("transaction_hour", pa.int64()),
            pa.field("day_of_week",      pa.int64()),
            pa.field("source_system",    pa.string()),
            pa.field("message_type",     pa.string()),
            pa.field("aml_pattern",      pa.string()),
            pa.field("_kafka_offset",    pa.int64()),
            pa.field("_kafka_partition", pa.int32()),
            pa.field("_kafka_topic",     pa.string()),
        ])

        def cast_to_schema(records_list):
            """
            Build a PyArrow table from records and cast to fixed schema.
            Missing columns are added as nulls.
            Extra columns are dropped.
            """
            # Build raw table from records
            raw_table = pa.Table.from_pylist(records_list)

            # Add any missing columns as null arrays
            for field in BRONZE_SCHEMA:
                if field.name not in raw_table.schema.names:
                    null_array = pa.array(
                        [None] * len(raw_table),
                        type=field.type
                    )
                    raw_table = raw_table.append_column(field, null_array)

            # Select only schema columns in correct order, then cast types
            raw_table = raw_table.select(
                [f.name for f in BRONZE_SCHEMA]
            )
            return raw_table.cast(BRONZE_SCHEMA)

        # ── Group records by their value_date ─────────────────────
        # Each Kafka message carries its own value_date (YYYY-MM-DD)
        # set by the producer — backdated for historical test data.
        # We partition by that date, not by datetime.now().
        from collections import defaultdict
        date_groups = defaultdict(list)

        for record in records:
            value_date = record.get("value_date", None)
            if value_date:
                try:
                    parts = value_date.split("-")
                    year  = parts[0]
                    month = parts[1].zfill(2)
                    day   = parts[2].zfill(2)
                except (IndexError, AttributeError):
                    now   = datetime.now(timezone.utc)
                    year  = now.strftime("%Y")
                    month = now.strftime("%m")
                    day   = now.strftime("%d")
            else:
                now   = datetime.now(timezone.utc)
                year  = now.strftime("%Y")
                month = now.strftime("%m")
                day   = now.strftime("%d")

            date_groups[(year, month, day)].append(record)

        # ── Write one parquet file per date partition ──────────────
        written_paths = []

        for (year, month, day), group_records in date_groups.items():

            partition_folder = os.path.join(
                dataset_name,
                f"year={year}",
                f"month={month}",
                f"day={day}",
            )

            # Microseconds in filename prevents collisions when
            # same date appears in multiple batches in same second
            ts       = datetime.now(timezone.utc).strftime("%H_%M_%S_%f")
            filename = f"{dataset_name}_{ts}.parquet"

            # Cast to fixed schema before writing
            try:
                table = cast_to_schema(group_records)
            except Exception as e:
                print(f"⚠️  Schema cast failed for partition "
                    f"{year}-{month}-{day}: {e}")
                print(f"   Falling back to raw write for {len(group_records)} records")
                table = pa.Table.from_pylist(group_records)

            if self.storage_type == "local":
                full_path = os.path.join(
                    self.bronze_path, partition_folder, filename
                )
                Path(os.path.join(
                    self.bronze_path, partition_folder
                )).mkdir(parents=True, exist_ok=True)
                pq.write_table(table, full_path)
                print(f"💾 Bronze written locally: "
                    f"{partition_folder}/{filename} "
                    f"({len(group_records)} records)")
                written_paths.append(full_path)

            elif self.storage_type == "azure":
                import io
                buffer = io.BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)

                file_path = f"{partition_folder}/{filename}"

                container_client = self.azure_client.get_file_system_client(
                    file_system="bronze"
                )

                try:
                    dir_client = container_client.get_directory_client(
                        partition_folder
                    )
                    dir_client.create_directory()
                except Exception:
                    pass  # Directory already exists

                file_client = container_client.get_file_client(file_path)
                file_client.upload_data(buffer.read(), overwrite=True)

                print(f"☁️  Bronze written to Azure: "
                    f"{file_path} ({len(group_records)} records)")
                written_paths.append(file_path)

        return written_paths