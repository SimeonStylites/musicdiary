import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()
cur.execute("ALTER TABLE listening_events ADD COLUMN IF NOT EXISTS album_name VARCHAR(500)")
conn.commit()

cur.close()
conn.close()