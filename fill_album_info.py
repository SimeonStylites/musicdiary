import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import time

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="user-read-recently-played"
    ))

def get_tracks_without_album_id(conn, limit=50):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT track_id, track_name, artist_name
        FROM listening_events
        WHERE track_id IS NOT NULL 
        AND (album_id IS NULL OR album_id = '')
        LIMIT %s
    """, (limit,))
    tracks = cur.fetchall()
    cur.close()
    return tracks

def update_track_album_id(conn, track_id, album_id):
    cur = conn.cursor()
    cur.execute("""
        UPDATE listening_events 
        SET album_id = %s 
        WHERE track_id = %s AND album_id IS NULL
    """, (album_id, track_id))
    conn.commit()
    cur.close()

def save_album_info(conn, album_id, album_name, artist_name, total_tracks, release_date):
    cur = conn.cursor()
    if release_date and len(release_date) == 4:
        release_date = f"{release_date}-01-01"  # 2004 -> 2004-01-01
    elif release_date and len(release_date) == 7:
        release_date = f"{release_date}-01"      # 2004-05 -> 2004-05-01
    cur.execute("""
        INSERT INTO albums (album_id, album_name, artist_name, total_tracks, release_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (album_id) DO NOTHING
    """, (album_id, album_name, artist_name, total_tracks, release_date))
    conn.commit()
    cur.close()

def main():
    load_dotenv()
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    sp = get_spotify_client()
    
    tracks = get_tracks_without_album_id(conn, limit=100)
    print(f"Tracks without album_id: {len(tracks)}")
    
    for track_id, track_name, artist_name in tracks:
        try:
            #Getting track info
            track_info = sp.track(track_id)
            album = track_info['album']
            album_id = album['id']
            album_name = album['name']
            artist_name_album = album['artists'][0]['name']
            total_tracks = album['total_tracks']
            release_date = album['release_date']
            
            #Saving album_id in listening_events
            update_track_album_id(conn, track_id, album_id)
            
            #Saving album info
            save_album_info(conn, album_id, album_name, artist_name_album, total_tracks, release_date)
            
            print(f"{track_name} -> {album_name} ({total_tracks} tracks)")
            time.sleep(2)  #Pause for API limits
            
        except Exception as e:
            print(f"Error for {track_name}: {e}")
    
    conn.close()

if __name__ == "__main__":
    main()