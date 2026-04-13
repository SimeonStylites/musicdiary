import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import time

load_dotenv()

#Connect to Spotify
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-recently-played"
))

#Connect to db
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

#Finding tracks with album=null
cur.execute("""
    SELECT DISTINCT track_id, track_name, artist_name
    FROM listening_events
    WHERE album_name IS NULL AND track_id IS NOT NULL
    LIMIT 100
""")
tracks = cur.fetchall()
print(f"Found tracks without allbums: {len(tracks)}")

updated = 0
for track_id, track_name, artist_name in tracks:
    try:
        #Getting info from Spotify API
        track_info = sp.track(track_id)
        album_name = track_info['album']['name']
        
        #Update all listening events
        cur.execute("""
            UPDATE listening_events 
            SET album_name = %s 
            WHERE track_id = %s AND album_name IS NULL
        """, (album_name, track_id))
        
        updated += cur.rowcount
        print(f"{track_name} - {artist_name} - {album_name}")
        time.sleep(0.1)  # pause because of api limits
        
    except Exception as e:
        print(f"Error {track_name}: {e}")

conn.commit()
print(f"\nUpdated events: {updated}")

cur.close()
conn.close()