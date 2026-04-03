import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

# 1. Top-5 tracks
cur.execute("""
    SELECT track_name, artist_name, COUNT(*) as plays
    FROM listening_events
    GROUP BY track_name, artist_name
    ORDER BY plays DESC
    LIMIT 5
""")
print("=== Top-5 tracks ===")
for row in cur.fetchall():
    print(f"{row[0]} - {row[1]} ({row[2]} plays)")

# 2. General Statistics
cur.execute("""
    SELECT 
        COUNT(*) as total_plays,
        COUNT(DISTINCT track_id) as unique_tracks
    FROM listening_events
""")
total, unique = cur.fetchone()
print(f"\n=== General statistics ===")
print(f"Total plays: {total}")
print(f"Total unique plays: {unique}")

cur.close()
conn.close()