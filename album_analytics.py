import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

#1.Top10 albums
print("=== Top-10 albums ===")
cur.execute("""
    SELECT album_name, artist_name, COUNT(*) as plays
    FROM listening_events
    WHERE album_name IS NOT NULL
    GROUP BY album_name, artist_name
    ORDER BY plays DESC
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"  {row[0]} - {row[1]} ({row[2]} plays)")

#2.Listened albums
#TODO make albums table, with total_tracks
print("\n=== Albums, you've listened ===")
cur.execute("""
    WITH album_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as total_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    ),
    listened_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as listened_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    )
    SELECT at.album_name, at.artist_name, at.total_tracks
    FROM album_tracks at
    JOIN listened_tracks lt ON at.album_name = lt.album_name AND at.artist_name = lt.artist_name
    WHERE at.total_tracks = lt.listened_tracks
    ORDER BY at.total_tracks DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]} - {row[1]} ({row[2]} tracks)")

#3.1-2 tracks missing
print("\n=== Albums without 1-2 tracks ===")
cur.execute("""
    WITH album_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as total_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    ),
    listened_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as listened_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    )
    SELECT at.album_name, at.artist_name, at.total_tracks, lt.listened_tracks,
           at.total_tracks - lt.listened_tracks as missing
    FROM album_tracks at
    JOIN listened_tracks lt ON at.album_name = lt.album_name AND at.artist_name = lt.artist_name
    WHERE at.total_tracks - lt.listened_tracks BETWEEN 1 AND 2
    ORDER BY missing, at.total_tracks DESC
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"  {row[0]} - {row[1]} (listened {row[3]}/{row[2]}, left {row[4]})")

#4.Started not finished
print("\n=== Albums started, but not finished (<30%) ===")
cur.execute("""
    WITH album_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as total_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    ),
    listened_tracks AS (
        SELECT album_name, artist_name, COUNT(DISTINCT track_id) as listened_tracks
        FROM listening_events
        WHERE album_name IS NOT NULL
        GROUP BY album_name, artist_name
    )
    SELECT at.album_name, at.artist_name, at.total_tracks, lt.listened_tracks,
           ROUND(100.0 * lt.listened_tracks / at.total_tracks, 1) as percent
    FROM album_tracks at
    JOIN listened_tracks lt ON at.album_name = lt.album_name AND at.artist_name = lt.artist_name
    WHERE lt.listened_tracks < 0.3 * at.total_tracks
    ORDER BY percent ASC
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"  {row[0]} - {row[1]} ({row[3]}/{row[2]} tracks, {row[4]}%)")

cur.close()
conn.close()