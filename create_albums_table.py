import psycopg2
from dotenv import load_dotenv
import os

def create_albums_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS albums (
            album_id VARCHAR(255) PRIMARY KEY,
            album_name VARCHAR(500),
            artist_name VARCHAR(500),
            total_tracks INTEGER,
            release_date DATE,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()

def main():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    create_albums_table(conn)
    conn.close()

if __name__ == "__main__":
    main()