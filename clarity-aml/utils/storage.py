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

        now = datetime.now(timezone.utc)
        
        # Build partition folder path (without container prefix)
        partition_folder = os.path.join(
            dataset_name,
            f"year={now.strftime('%Y')}",
            f"month={now.strftime('%m')}",
            f"day={now.strftime('%d')}",
        )

        ts        = now.strftime("%H_%M_%S")
        filename  = f"{dataset_name}_{ts}.parquet"
        table     = pa.Table.from_pylist(records)

        if self.storage_type == "local":
            # ── LOCAL ─────────────────────────────────────────────
            full_path = os.path.join(
                self.bronze_path, partition_folder, filename
            )
            Path(os.path.join(self.bronze_path, partition_folder)).mkdir(
                parents=True, exist_ok=True
            )
            pq.write_table(table, full_path)
            print(f"💾 Bronze written locally: {full_path} ({len(records)} records)")
            return full_path

        elif self.storage_type == "azure":
            # ── AZURE ─────────────────────────────────────────────
            # Convert table to parquet bytes in memory
            import io
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # File path INSIDE the container (no abfss:// prefix)
            # bronze container → transactions/year=2026/month=06/day=15/transactions_08_13_00.parquet
            file_path = f"{partition_folder}/{filename}"

            # Get the bronze container client directly
            container_client = self.azure_client.get_file_system_client(
                file_system="bronze"
            )

            # Create directory if it doesn't exist
            try:
                dir_client = container_client.get_directory_client(partition_folder)
                dir_client.create_directory()
            except Exception:
                pass  # Directory already exists — fine

            # Upload the file
            file_client = container_client.get_file_client(file_path)
            file_client.upload_data(buffer.read(), overwrite=True)

            print(f"☁️  Bronze written to Azure: {file_path} ({len(records)} records)")
            return file_path

    def write_silver(self, records: list, dataset_name: str):
        """Write enriched records to Silver layer."""
        # Same logic as write_bronze but to silver path
        # In a full implementation, silver would also
        # validate schema and apply transformations
        partition_path = self._get_partition_path(
            self.silver_path, dataset_name
        )
        table    = pa.Table.from_pylist(records)
        ts       = datetime.now(timezone.utc).strftime("%H_%M_%S")
        filename = f"{dataset_name}_{ts}.parquet"
        full_path = os.path.join(partition_path, filename)

        if self.storage_type == "local":
            Path(partition_path).mkdir(parents=True, exist_ok=True)
            pq.write_table(table, full_path)

        print(f"💾 Silver written: {full_path} ({len(records)} records)")
        return full_path