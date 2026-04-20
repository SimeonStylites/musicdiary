import psycopg2
from dotenv import load_dotenv
import os

def add_album_id_column(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE listening_events 
        ADD COLUMN IF NOT EXISTS album_id VARCHAR(255)
    """)
    conn.commit()
    cur.close()
    print("album_id column added to listening events")

def main():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    add_album_id_column(conn)
    conn.close()

if __name__ == "__main__":
    main()