import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_or_create_artist(conn, artist_name):
    cur = conn.cursor()
    cur.execute("SELECT artist_id FROM artists WHERE artist_name = %s", (artist_name,))
    row = cur.fetchone()
    if row:
        cur.close()
        return row[0]
    cur.execute("INSERT INTO artists (artist_name) VALUES (%s) RETURNING artist_id", (artist_name,))
    artist_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return artist_id

def get_or_create_album(conn, artist_id, album_name, spotify_album_id=None, total_tracks=None, release_date=None):
    cur = conn.cursor()
    cur.execute("SELECT album_id FROM albums WHERE artist_id = %s AND album_name = %s", (artist_id, album_name))
    row = cur.fetchone()
    if row:
        cur.close()
        return row[0]
    cur.execute("""
        INSERT INTO albums (artist_id, album_name, spotify_album_id, total_tracks, release_date)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING album_id
    """, (artist_id, album_name, spotify_album_id, total_tracks, release_date))
    album_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return album_id

def event_exists(conn, played_at):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM listening_events WHERE played_at = %s", (played_at,))
    exists = cur.fetchone() is not None
    cur.close()
    return exists

def insert_event(conn, played_at, track_id, track_name, album_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO listening_events (played_at, track_id, track_name, album_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (played_at) DO NOTHING
    """, (played_at, track_id, track_name, album_id))
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    return inserted
