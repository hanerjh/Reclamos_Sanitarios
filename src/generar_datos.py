
import pandas as pd
import random
import hashlib
from faker import Faker
import os

fake = Faker()

def generate_patients(n=1000):
    data = []
    for i in range(n):
        data.append({
            "patient_id": i,
            "name": fake.name(),
            "ssn": fake.ssn(),
            "birth_date": fake.date_of_birth().isoformat(),
            "year": random.choice([2022, 2023, 2024]),
            "month": random.randint(1, 12)
        })
    return pd.DataFrame(data)

def generate_claims(n=5000):
    data = []
    for i in range(n):
        data.append({
            "claim_id": i,
            "patient_id": random.randint(0, 999),
            "provider_id": random.randint(1, 50),
            "amount": round(random.uniform(50, 5000), 2),
            "year_claim": random.choice([2022, 2023, 2024]),
            "month_claim": random.randint(1, 12)
        })
    return pd.DataFrame(data)

patients = generate_patients()
claims = generate_claims()

#os.makedirs("data/raw", exist_ok=True)
patients.to_csv("/Volumes/Workspace/default/reclamos_sanitarios/raw_data/patients.csv", index=False)
claims.to_csv("/Volumes/Workspace/default/reclamos_sanitarios/raw_data/claims.csv", index=False)
