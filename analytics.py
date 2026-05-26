import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def run_query(conn, query, params=None):
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()

def print_section(title, results, headers):
    print(f"\n{'='*50}")
    print(f"{title}")
    print('='*50)
    #Headers
    header_line = "  ".join([f"{h:<25}" for h in headers])
    print(header_line)
    print("-" * len(header_line))
    for row in results:
        row_str = "  ".join([f"{str(col)[:25]:<25}" for col in row])
        print(row_str)

def main():
    conn = get_connection()
    
    #1.General
    results = run_query(conn, """
        SELECT 
            COUNT(*) as total_plays,
            COUNT(DISTINCT le.track_id) as unique_tracks,
            COUNT(DISTINCT a.album_id) as unique_albums,
            COUNT(DISTINCT ar.artist_id) as unique_artists
        FROM listening_events le
        JOIN albums a ON le.album_id = a.album_id
        JOIN artists ar ON a.artist_id = ar.artist_id
    """)
    print_section("General Stats", results, 
                  ["Total listened", "Unique Tracks", "Unique albums", "Unique artists"])
    
    #2.Top artists
    results = run_query(conn, """
        SELECT ar.artist_name, COUNT(*) as plays
        FROM listening_events le
        JOIN albums a ON le.album_id = a.album_id
        JOIN artists ar ON a.artist_id = ar.artist_id
        GROUP BY ar.artist_name
        ORDER BY plays DESC
        LIMIT 10
    """)
    print_section("Top-10 artists", results, ["Artist", "Listened"])
    
    #3.Top tracks
    results = run_query(conn, """
        SELECT le.track_name, ar.artist_name, COUNT(*) as plays
        FROM listening_events le
        JOIN albums a ON le.album_id = a.album_id
        JOIN artists ar ON a.artist_id = ar.artist_id
        GROUP BY le.track_name, ar.artist_name
        ORDER BY plays DESC
        LIMIT 10
    """)
    print_section("Top-10 tracks", results, ["Track", "Artist", "Listened"])
    
    #4. Activity by hour
    results = run_query(conn, """
        SELECT EXTRACT(HOUR FROM le.played_at) as hour, COUNT(*) as plays
        FROM listening_events le
        GROUP BY hour
        ORDER BY hour
    """)
    print_section("Activity by hour", results, ["Hour", "Listened"])
    
    #5.Activity by day of week
    results = run_query(conn, """
        SELECT 
            TO_CHAR(le.played_at, 'Day') as day_name,
            EXTRACT(DOW FROM le.played_at) as day_num,
            COUNT(*) as plays
        FROM listening_events le
        GROUP BY day_name, day_num
        ORDER BY day_num
    """)
    print_section("Activity by dow", results, ["Day", "Listened"])
    
    #6. Top-artists in last 30 days
    results = run_query(conn, """
        SELECT ar.artist_name, COUNT(*) as plays
        FROM listening_events le
        JOIN albums a ON le.album_id = a.album_id
        JOIN artists ar ON a.artist_id = ar.artist_id
        WHERE le.played_at > NOW() - INTERVAL '30 days'
        GROUP BY ar.artist_name
        ORDER BY plays DESC
        LIMIT 10
    """)
    print_section("Top-artists in last 30 days", results, ["Artist", "Listened"])
    
    #7. Entirely listened albums
    results = run_query(conn, """
        WITH album_tracks AS (
            SELECT a.album_id, a.album_name, a.artist_id, a.total_tracks
            FROM albums a
            WHERE a.total_tracks IS NOT NULL
        ),
        listened_tracks AS (
            SELECT le.album_id, COUNT(DISTINCT le.track_id) as listened_count
            FROM listening_events le
            GROUP BY le.album_id
        )
        SELECT at.album_name, ar.artist_name, at.total_tracks, lt.listened_count
        FROM album_tracks at
        JOIN listened_tracks lt ON at.album_id = lt.album_id
        JOIN artists ar ON at.artist_id = ar.artist_id
        WHERE at.total_tracks = lt.listened_count
        ORDER BY at.total_tracks DESC
        LIMIT 50
    """)
    print_section("Entirely listened albums", results, 
                  ["Album", "Artist", "Tracks", "Tracks listened"])
    
    #8. Albums without 1-2 tracks
    results = run_query(conn, """
        WITH album_tracks AS (
            SELECT a.album_id, a.album_name, a.artist_id, a.total_tracks
            FROM albums a
            WHERE a.total_tracks IS NOT NULL
        ),
        listened_tracks AS (
            SELECT le.album_id, COUNT(DISTINCT le.track_id) as listened_count
            FROM listening_events le
            GROUP BY le.album_id
        )
        SELECT at.album_name, ar.artist_name, at.total_tracks, lt.listened_count,
               at.total_tracks - lt.listened_count as missing
        FROM album_tracks at
        JOIN listened_tracks lt ON at.album_id = lt.album_id
        JOIN artists ar ON at.artist_id = ar.artist_id
        WHERE at.total_tracks - lt.listened_count BETWEEN 1 AND 2
        ORDER BY missing, at.total_tracks DESC
        LIMIT 50
    """)
    print_section("Albums without 1-2 tracks", results,
                  ["Album", "Artist", "Tracks", "Tracks listened", "Tracks left"])
    
    conn.close()

if __name__ == "__main__":
    main()