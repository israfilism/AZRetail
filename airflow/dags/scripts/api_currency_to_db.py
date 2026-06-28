import os
import uuid
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
from zoneinfo import ZoneInfo

baku_tz = ZoneInfo("Asia/Baku")


def ingest_api_currency(target_table="bronze_currency"):
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(baku_tz).replace(tzinfo=None)
    
    url = "https://api.exchangerate-api.com/v4/latest/AZN"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        api_data = response.json()
    except Exception as e:
        raise RuntimeError(f"error: {e}")
        
    rates = api_data.get("rates", {})
    if not rates:
        raise ValueError()

    currency_rows = []
    for currency_code, rate in rates.items():
        rate_to_azn = round(1 / rate, 6) if rate > 0 else 0 
        
        currency_rows.append({
            "currency_code": str(currency_code).upper().strip(),
            "rate_from_azn": str(rate),
            "rate_to_azn": str(rate_to_azn)
        })
        
    df = pd.DataFrame(currency_rows)
    
    df.insert(0, '_batch_id', str(batch_id))
    df.insert(1, '_ingested_at', str(ingested_at))
    df.insert(2, '_source_name', str(url))
    
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db = os.getenv("POSTGRES_DB", "airflow")
    
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@postgres:5432/{db}")
    

    df.to_sql(
        name=f"{target_table}",
        con=engine,
        schema='datalake',
        if_exists='replace',
        index=False
    )

if __name__ == "__main__":
    ingest_api_currency()