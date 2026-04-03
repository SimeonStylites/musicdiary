import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS listening_events (
        id SERIAL PRIMARY KEY,
        played_at TIMESTAMP UNIQUE,
        track_id VARCHAR(255),
        track_name VARCHAR(500),
        artist_name VARCHAR(500),
        created_at TIMESTAMP DEFAULT NOW()
    )
""")

conn.commit()
cur.close()
conn.close()

print("Table 'listening_events' created")