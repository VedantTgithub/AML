"""
Loads real names from reference files so the producer
can inject them into transactions — guaranteeing matches
in ADF entity resolution.
"""

import csv
import random
import os

class ReferenceLoader:
    
    def __init__(self):
        base = os.path.join(
            os.path.dirname(__file__), '..', '..', 'data', 'reference'
        )
        
        self.kvk_names      = self._load_kvk_names(
            os.path.join(base, 'kvk_companies.csv')
        )
        self.sanctions_names = self._load_sanctions_names(
            os.path.join(base, 'ofac_sdn.csv')
        )
        
        print(f"✅ Loaded {len(self.kvk_names):,} KvK company names")
        print(f"✅ Loaded {len(self.sanctions_names):,} sanctions names")

    def _load_kvk_names(self, path):
        """Load company names from synthetic KvK CSV."""
        names = []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('status') == 'active':
                        names.append(row['company_name'])
        except FileNotFoundError:
            print(f"⚠️  KvK file not found: {path}")
        return names

    def _load_sanctions_names(self, path):
        """
        Load sanctioned entity names from OFAC SDN CSV.
        Column_2 = entity name (no headers in OFAC file).
        """
        names = []
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        name = row[1].strip()
                        # Filter out empty and -0- entries
                        if name and name != '-0-' and len(name) > 3:
                            names.append(name)
        except FileNotFoundError:
            print(f"⚠️  OFAC file not found: {path}")
        return names

    def get_kvk_company_name(self):
        """Return a real KvK registered company name."""
        if self.kvk_names:
            return random.choice(self.kvk_names)
        return "Berg Logistics BV"

    def get_sanctioned_name(self):
        """Return a real sanctioned entity name."""
        if self.sanctions_names:
            return random.choice(self.sanctions_names)
        return "BANCO NACIONAL DE CUBA"