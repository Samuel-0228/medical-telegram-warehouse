# scripts/load_raw.py
import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import glob

load_dotenv()

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)


def load_to_raw():
    raw_dir = 'data/raw/telegram_messages'
    all_files = glob.glob(os.path.join(
        raw_dir, '**', '*.json'), recursive=True)

    for file_path in all_files:
        with open(file_path, 'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        if not df.empty:
            # Partition by date/channel in table name or use a single table
            df.to_sql('telegram_messages', engine, schema='raw',
                      if_exists='append', index=False)
            print(f"Loaded {len(df)} rows from {file_path}")


if __name__ == "__main__":
    load_to_raw()
