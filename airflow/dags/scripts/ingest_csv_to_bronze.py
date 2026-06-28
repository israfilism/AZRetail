import os
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from zoneinfo import ZoneInfo

baku_tz = ZoneInfo("Asia/Baku")


def ingest_csv(file_name, target_table):
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(baku_tz).replace(tzinfo=None)
    
    if not os.path.exists(file_name):
        raise FileNotFoundError()
        
    df = pd.read_csv(file_name, dtype=str)
    
    df.insert(0, '_batch_id', batch_id)
    df.insert(1, '_ingested_at', ingested_at)
    df.insert(2, '_source_name', file_name)
    
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