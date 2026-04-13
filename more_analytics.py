import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def top_artists(limit=10):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT artist_name, COUNT(*) as plays
        FROM listening_events
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT %s
    """, (limit,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def activity_by_hour():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT EXTRACT(HOUR FROM played_at) as hour, COUNT(*) as plays
        FROM listening_events
        GROUP BY hour
        ORDER BY hour
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def weekday_activity():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            TO_CHAR(played_at, 'Day') as day_name,
            EXTRACT(DOW FROM played_at) as day_num,
            COUNT(*) as plays
        FROM listening_events
        GROUP BY day_name, day_num
        ORDER BY day_num
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def recent_top_artists(days=30, limit=10):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT artist_name, COUNT(*) as plays
        FROM listening_events
        WHERE played_at > NOW() - INTERVAL '%s days'
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT %s
    """, (days, limit))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def daily_stats(limit=10):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            DATE(played_at) as date,
            COUNT(*) as plays,
            COUNT(DISTINCT track_id) as unique_tracks
        FROM listening_events
        GROUP BY DATE(played_at)
        ORDER BY plays DESC
        LIMIT %s
    """, (limit,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

if __name__ == "__main__":
    print("=== Top-10 artists ===")
    for row in top_artists(10):
        print(f"  {row[0]} - {row[1]} plays")
    
    print("\n=== Activity by hour ===")
    for row in activity_by_hour():
        print(f"  {int(row[0])}:00 - {row[1]} plays")
    
    print("\n=== Days of the week ===")
    for row in weekday_activity():
        print(f"  {row[0].strip()} - {row[2]} plays")
    
    print("\n=== Top artists in last 30 days ===")
    for row in recent_top_artists(30, 10):
        print(f"  {row[0]} - {row[1]} plays")
    
    print("\n=== Top days ===")
    for row in daily_stats(10):
        print(f"  {row[0]} - {row[1]} plays, {row[2]} unique tracks")