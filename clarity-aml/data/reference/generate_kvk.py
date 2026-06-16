"""
Synthetic Dutch KvK (Chamber of Commerce) Company Registry
Simulates the real KvK dataset structure.
Real KvK: kvk.nl/english/how-to-use-the-business-register
"""

import csv
import random
import uuid
from faker import Faker

fake = Faker('nl_NL')

LEGAL_FORMS = [
    "BV", "NV", "VOF", "CV", "Eenmanszaak",
    "Stichting", "Vereniging", "Cooperatie"
]

SECTORS = [
    "4771", "4941", "6201", "6419", "6820",
    "7022", "8299", "4649", "5610", "6110"
]

SECTOR_NAMES = {
    "4771": "Retail clothing",
    "4941": "Road freight transport",
    "6201": "Software development",
    "6419": "Financial services",
    "6820": "Real estate",
    "7022": "Business consultancy",
    "8299": "Business support services",
    "4649": "Wholesale household goods",
    "5610": "Restaurants",
    "6110": "Telecommunications"
}

DUTCH_CITIES = [
    "Amsterdam", "Rotterdam", "Den Haag", "Utrecht",
    "Eindhoven", "Groningen", "Tilburg", "Almere",
    "Breda", "Nijmegen", "Haarlem", "Arnhem"
]

def generate_kvk_number():
    """KvK numbers are 8 digits."""
    return str(random.randint(10000000, 99999999))

def generate_companies(count=50000):
    companies = []

    for _ in range(count):
        sector_code = random.choice(SECTORS)
        legal_form  = random.choice(LEGAL_FORMS)
        city        = random.choice(DUTCH_CITIES)
        name_base   = fake.last_name()
        sector_name = SECTOR_NAMES[sector_code]

        companies.append({
            "kvk_number":         generate_kvk_number(),
            "company_name":       f"{name_base} {sector_name} {legal_form}",
            "legal_form":         legal_form,
            "sector_code":        sector_code,
            "sector_description": sector_name,
            "city":               city,
            "postcode":           fake.postcode(),
            "street":             fake.street_name(),
            "house_number":       str(random.randint(1, 999)),
            "registration_date":  fake.date_between(
                                    start_date="-20y",
                                    end_date="today"
                                  ).strftime("%Y-%m-%d"),
            "status":             random.choices(
                                    ["active", "active", "active", "dissolved"],
                                    weights=[85, 85, 85, 15]
                                  )[0],
            "annual_revenue_eur": random.randint(50000, 50000000),
            "employee_count":     random.randint(1, 5000),
        })

    return companies

def save_to_csv(companies, output_path):
    fieldnames = companies[0].keys()
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(companies)
    print(f"✅ Generated {len(companies):,} companies → {output_path}")

if __name__ == "__main__":
    import os
    os.makedirs("../reference", exist_ok=True)
    companies = generate_companies(50000)
    save_to_csv(companies, "../reference/kvk_companies.csv")
    print("Done. Upload this file to ADLS bronze/reference/kvk/")