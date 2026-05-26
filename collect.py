import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="user-read-recently-played"
    ))

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

def get_or_create_album(conn, artist_id, album_name, spotify_album_id, total_tracks, release_date):
    cur = conn.cursor()
    cur.execute("SELECT album_id FROM albums WHERE artist_id = %s AND album_name = %s", (artist_id, album_name))
    row = cur.fetchone()
    if row:
        cur.close()
        return row[0]
    
    if release_date and len(release_date) == 4:
        release_date = f"{release_date}-01-01"
    elif release_date and len(release_date) == 7:
        release_date = f"{release_date}-01"
    
    cur.execute("""
        INSERT INTO albums (artist_id, album_name, spotify_album_id, total_tracks, release_date)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING album_id
    """, (artist_id, album_name, spotify_album_id, total_tracks, release_date))
    album_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return album_id

def save_listening_event(conn, played_at, track_id, track_name, album_id):
    cur = conn.cursor()
    # Проверяем, есть ли уже запись
    cur.execute("SELECT 1 FROM listening_events WHERE played_at = %s", (played_at,))
    if cur.fetchone():
        cur.close()
        return 0
    
    cur.execute("""
        INSERT INTO listening_events (played_at, track_id, track_name, album_id)
        VALUES (%s, %s, %s, %s)
    """, (played_at, track_id, track_name, album_id))
    conn.commit()
    cur.close()
    return 1

def main():
    conn = get_connection()
    sp = get_spotify_client()
    
    results = sp.current_user_recently_played(limit=50)
    
    saved = 0
    for item in results['items']:
        track = item['track']
        track_id = track['id']
        track_name = track['name']
        artist_name = track['artists'][0]['name']
        played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))
        
        album = track['album']
        spotify_album_id = album['id']
        album_name = album['name']
        total_tracks = album['total_tracks']
        release_date = album['release_date']
        
        artist_id = get_or_create_artist(conn, artist_name)
        album_id = get_or_create_album(conn, artist_id, album_name, spotify_album_id, total_tracks, release_date)
        
        if save_listening_event(conn, played_at, track_id, track_name, album_id):
            saved += 1
    
    conn.close()
    print(f"Saved {saved} new listening events (total in response: {len(results['items'])})")

if __name__ == "__main__":
    main()