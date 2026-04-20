import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

#Connecting to Spotify
def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="user-read-recently-played"
    ))


def save_album(conn, album_id, album_name, artist_name, total_tracks, release_date):
    cur = conn.cursor()
    #Change release_date format
    if release_date and len(release_date) == 4:
        release_date = f"{release_date}-01-01"
    elif release_date and len(release_date) == 7:
        release_date = f"{release_date}-01"
    
    cur.execute("""
        INSERT INTO albums (album_id, album_name, artist_name, total_tracks, release_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (album_id) DO NOTHING
    """, (album_id, album_name, artist_name, total_tracks, release_date))
    conn.commit()
    cur.close()

def save_listening_event(conn, played_at, track_id, track_name, artist_name, album_name, album_id):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO listening_events (played_at, track_id, track_name, artist_name, album_name, album_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (played_at) DO NOTHING
    """, (played_at, track_id, track_name, artist_name, album_name, album_id))
    conn.commit()
    cur.close()

def main():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    sp = get_spotify_client()
    
    results = sp.current_user_recently_played(limit=50)
    
    saved = 0
    for item in results['items']:
        track = item['track']
        track_id = track['id']
        track_name = track['name']
        artist_name = track['artists'][0]['name']
        played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))
        
        #Getting album info
        album = track['album']
        album_id = album['id']
        album_name = album['name']
        total_tracks = album['total_tracks']
        release_date = album['release_date']

        save_album(conn, album_id, album_name, artist_name, total_tracks, release_date)
        
        save_listening_event(conn, played_at, track_id, track_name, artist_name, album_name, album_id)
        
        saved += 1
    
    conn.close()
    print(f"Saved {saved} new events")

if __name__ == "__main__":
    main()