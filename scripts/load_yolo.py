# scripts/load_yolo.py
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

df = pd.read_csv('data/yolo_detections.csv')
df.to_sql('yolo_detections', engine, schema='raw',
          if_exists='replace', index=False)
print(f"Loaded {len(df)} YOLO detections to raw.yolo_detections")
