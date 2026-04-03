import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    print("Connected to PostgreSQL!")
    conn.close()
except Exception as e:
    print(f"Error: {e}")