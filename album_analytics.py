import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def print_section(title, results, headers):
    print(f"\n{'='*60}")
    print(f"=== {title}")
    print('='*60)
    if not results:
        print("  (no data)")
        return
    header_line = "  ".join([f"{h:<35}" for h in headers])
    print(header_line)
    print("-" * len(header_line))
    for row in results:
        row_str = "  ".join([f"{str(col)[:35]:<35}" for col in row])
        print(row_str)

def main():
    conn = get_connection()
    cur = conn.cursor()

    #1. All entirely listened albums
    cur.execute("""
        WITH album_stats AS (
            SELECT 
                a.album_id,
                a.album_name,
                a.total_tracks,
                ar.artist_name,
                COUNT(DISTINCT le.track_id) as listened_tracks
            FROM albums a
            JOIN artists ar ON a.artist_id = ar.artist_id
            JOIN listening_events le ON a.album_id = le.album_id
            WHERE a.total_tracks IS NOT NULL AND a.total_tracks > 0
            GROUP BY a.album_id, a.album_name, a.total_tracks, ar.artist_name
        )
        SELECT album_name, artist_name, total_tracks, listened_tracks
        FROM album_stats
        WHERE listened_tracks = total_tracks
        ORDER BY total_tracks DESC
    """)
    results = cur.fetchall()
    print_section("Entirely listened albums", 
                  results, ["Album", "Artist", "Tracks", "Tracks listened"])

    #2. Same with 5+ tracks
    cur.execute("""
        WITH album_stats AS (
            SELECT 
                a.album_id,
                a.album_name,
                a.total_tracks,
                ar.artist_name,
                COUNT(DISTINCT le.track_id) as listened_tracks
            FROM albums a
            JOIN artists ar ON a.artist_id = ar.artist_id
            JOIN listening_events le ON a.album_id = le.album_id
            WHERE a.total_tracks IS NOT NULL AND a.total_tracks >= 5
            GROUP BY a.album_id, a.album_name, a.total_tracks, ar.artist_name
        )
        SELECT album_name, artist_name, total_tracks, listened_tracks
        FROM album_stats
        WHERE listened_tracks = total_tracks
        ORDER BY total_tracks DESC
    """)
    results = cur.fetchall()
    print_section("Entirely listened albums (5+ Tracks)", 
                  results, ["Album", "Artist", "Tracks", "Listened tracks"])

    #3.How many entirely listened (by length)
    cur.execute("""
        WITH album_stats AS (
            SELECT 
                a.album_id,
                a.total_tracks,
                COUNT(DISTINCT le.track_id) as listened_tracks,
                CASE 
                    WHEN a.total_tracks = 1 THEN '1 track'
                    WHEN a.total_tracks BETWEEN 2 AND 4 THEN '2-4 tracks'
                    WHEN a.total_tracks BETWEEN 5 AND 10 THEN '5-10 tracks'
                    ELSE '10+ tracks'
                END as album_size
            FROM albums a
            JOIN listening_events le ON a.album_id = le.album_id
            WHERE a.total_tracks IS NOT NULL
            GROUP BY a.album_id, a.total_tracks
        )
        SELECT album_size, COUNT(*) as fully_listened
        FROM album_stats
        WHERE listened_tracks = total_tracks
        GROUP BY album_size
        ORDER BY 
            CASE album_size
                WHEN '1 track' THEN 1
                WHEN '2-4 tracks' THEN 2
                WHEN '5-10 tracks' THEN 3
                ELSE 4
            END
    """)
    results = cur.fetchall()
    print_section("How many entirely listened albums", 
                  results, ["Length", "Number of entirely listened"])

    #4.Artists with most number of listened albums
    cur.execute("""
        WITH album_stats AS (
            SELECT 
                a.album_id,
                ar.artist_name,
                a.total_tracks,
                COUNT(DISTINCT le.track_id) as listened_tracks
            FROM albums a
            JOIN artists ar ON a.artist_id = ar.artist_id
            JOIN listening_events le ON a.album_id = le.album_id
            WHERE a.total_tracks IS NOT NULL AND a.total_tracks >= 3
            GROUP BY a.album_id, a.total_tracks, ar.artist_name
            HAVING COUNT(DISTINCT le.track_id) = a.total_tracks
        )
        SELECT artist_name, COUNT(*) as fully_listened_albums
        FROM album_stats
        GROUP BY artist_name
        ORDER BY fully_listened_albums DESC
        LIMIT 15
    """)
    results = cur.fetchall()
    print_section("Top-15 artists with entirely listened albums", 
                  results, ["Artist", "Entirely listened albums"])

    #5. Albums without 1 track
    cur.execute("""
        WITH album_stats AS (
            SELECT 
                a.album_name,
                ar.artist_name,
                a.total_tracks,
                COUNT(DISTINCT le.track_id) as listened_tracks
            FROM albums a
            JOIN artists ar ON a.artist_id = ar.artist_id
            JOIN listening_events le ON a.album_id = le.album_id
            WHERE a.total_tracks IS NOT NULL AND a.total_tracks >= 5
            GROUP BY a.album_name, a.total_tracks, ar.artist_name
        )
        SELECT album_name, artist_name, total_tracks, listened_tracks
        FROM album_stats
        WHERE total_tracks - listened_tracks = 1
        ORDER BY total_tracks DESC
        LIMIT 20
    """)
    results = cur.fetchall()
    print_section("Albums without 1 track (5+ tracks)", 
                  results, ["Album", "Artist", "Total tracks", "Listened"])

    #7.Top albums
    cur.execute("""
        WITH track_plays AS (
            SELECT 
                a.album_id,
                a.album_name,
                ar.artist_name,
                le.track_id,
                COUNT(*) as plays
            FROM listening_events le
            JOIN albums a ON le.album_id = a.album_id
            JOIN artists ar ON a.artist_id = ar.artist_id
            GROUP BY a.album_id, a.album_name, ar.artist_name, le.track_id
        ),
        album_min_plays AS (
            SELECT 
                album_id,
                album_name,
                artist_name,
                MIN(plays) as min_track_plays,
                COUNT(DISTINCT track_id) as tracks_in_album
            FROM track_plays
            WHERE plays > 0
            GROUP BY album_id, album_name, artist_name
        )
        SELECT album_name, artist_name, min_track_plays, tracks_in_album
        FROM album_min_plays
        WHERE tracks_in_album >= 3
        ORDER BY min_track_plays DESC
        LIMIT 15
    """)
    results = cur.fetchall()
    print_section("Top albums", 
                results, ["Album", "Artist", "Listened", "Tracks"])

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()