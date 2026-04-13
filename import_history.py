import json
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

DATA_FOLDER = "my_spotify_data\Spotify Extended Streaming History"

json_files = list(Path(DATA_FOLDER).glob("Streaming_History_Audio_*.json"))
print(f"{len(json_files)} files found")

total_inserted = 0
total_skipped = 0

for file_path in json_files:
    print(f"Processing {file_path.name}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        tracks = json.load(f)
    
    for track in tracks:
        #Skip if podcast
        if track.get("episode_name") is not None:
            continue
        
        #Data extracting
        played_at_str = track.get("ts")
        if not played_at_str:
            continue
            
        played_at = datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))
        
        #Extracting track_id from URI (spotify:track:ID)
        track_uri = track.get("spotify_track_uri", "")
        track_id = track_uri.split(":")[-1] if track_uri else None
        
        track_name = track.get("master_metadata_track_name")
        artist_name = track.get("master_metadata_album_artist_name")
        album_name = track.get("master_metadata_album_album_name")
        
        #Skip if no name or artist
        if not track_name or not artist_name:
            continue
        
        #Enter to the db
        cur.execute("""
            INSERT INTO listening_events (played_at, track_id, track_name, artist_name, album_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (played_at) DO NOTHING
        """, (played_at, track_id, track_name, artist_name, album_name))
        
        if cur.rowcount > 0:
            total_inserted += 1
        else:
            total_skipped += 1

conn.commit()
cur.close()
conn.close()

print(f"Added: {total_inserted}")
print(f"Skipped: {total_skipped}")