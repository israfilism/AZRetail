import os
import json
import uuid
import time
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from zoneinfo import ZoneInfo

baku_tz = ZoneInfo("Asia/Baku")


def fetch_page_with_retry(pages_list, target_page, max_retries=3, backoff_factor=2, timeout_seconds=10):
    attempt = 0
    while attempt < max_retries:
        try:         
            start_time = time.time()
            page_data = next((p for p in pages_list if p['page'] == target_page), None)
            if (time.time() - start_time) > timeout_seconds:
                raise TimeoutError(f"API connection timed out {target_page}")
            if not page_data:
                raise ValueError()
            return page_data
            
        except (ValueError, TimeoutError) as e:
            attempt += 1
            if attempt == max_retries:
                raise e
                
            sleep_time = backoff_factor ** attempt
            time.sleep(sleep_time)

def ingest_json(file_name, target_table, ds):
    
    if not os.path.exists(file_name):
        raise FileNotFoundError()
        
    with open(file_name, 'r') as f:
        raw_json = json.load(f)
        
    pages_list = raw_json.get("pages", [])
    
    all_orders = []
    current_page = 1
    
    while current_page is not None:
        page_payload = fetch_page_with_retry(
            pages_list=pages_list, 
            target_page=current_page,
            max_retries=3,
            backoff_factor=2,
            timeout_seconds=5
        )
        orders_in_page = page_payload.get("data", [])
        for order in orders_in_page:
            order_date = order.get("order_ts", "")[:10] 
            if order_date == ds:
                all_orders.append(order)
        current_page = page_payload.get("next_page")
        
    if not all_orders:
        return

    df = pd.DataFrame(all_orders)
    
    if 'items' in df.columns and not df['items'].isna().all():
        df = df.dropna(subset=['items'])
        df = df.explode('items').reset_index(drop=True)
        items_df = pd.json_normalize(df['items'])
        df = df.drop(columns=['items']).join(items_df)
        
    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(baku_tz).replace(tzinfo=None)
    
    df.insert(0, '_batch_id', batch_id)
    df.insert(1, '_ingested_at', ingested_at)
    df.insert(2, '_source_name', file_name)
    
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db = os.getenv("POSTGRES_DB", "airflow")
    
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@postgres:5432/{db}")
    
    with engine.begin() as connection:
        
        check_table_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'datalake' AND table_name = :table
            );
        """)
        table_exists = connection.execute(check_table_query, {"table": target_table}).scalar()
        
        if table_exists:
            delete_query = text(f"DELETE FROM datalake.{target_table} WHERE DATE(order_ts) = :ds")
            connection.execute(delete_query, {"ds": ds})
            
    df.to_sql(
        name=target_table,
        con=engine, 
        schema='datalake',
        if_exists='append',
        index=False
    )