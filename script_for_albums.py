import psycopg2
from dotenv import load_dotenv
import os
import json
from pathlib import Path

load_dotenv()
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

# Added album name in every string
DATA_FOLDER = "my_spotify_data\Spotify Extended Streaming History"
json_files = list(Path(DATA_FOLDER).glob("Streaming_History_Audio_*.json"))

updated = 0
for file_path in json_files:
    with open(file_path, 'r', encoding='utf-8') as f:
        tracks = json.load(f)
    
    for track in tracks:
        played_at_str = track.get("ts")
        if not played_at_str:
            continue
        
        from datetime import datetime
        played_at = datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))
        album_name = track.get("master_metadata_album_album_name")
        
        if album_name:
            cur.execute("""
                UPDATE listening_events 
                SET album_name = %s 
                WHERE played_at = %s AND album_name IS NULL
            """, (album_name, played_at))
            updated += cur.rowcount

conn.commit()
print(f"Updated plays:{updated}")

cur.close()
conn.close()