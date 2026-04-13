import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

#Connecting to Spotify
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
    scope="user-read-recently-played"
))

#Connecting to PostgreSQL
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

#Getting last 20 tracks
results = sp.current_user_recently_played(limit=50)

saved_events = 0
for item in results['items']:
    track = item['track']
    track_id = track['id']
    played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))
    
    try:
        #Saving the event
        cur.execute("""
            INSERT INTO listening_events (played_at, track_id, track_name, artist_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (played_at) DO NOTHING
        """, (played_at, track_id, track['name'], track['artists'][0]['name']))
        
        if cur.rowcount > 0:
            saved_events += 1
    except Exception as e:
        print(f"Error: {e}")

conn.commit()
cur.close()
conn.close()

print(f"Saved {saved_events} new events")