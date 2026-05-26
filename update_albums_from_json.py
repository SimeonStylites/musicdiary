import psycopg2
import json
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_json_files(data_folder="my_spotify_data/Spotify Extended Streaming History"):
    """Return lsit of JSOn files with spotify history"""
    return list(Path(data_folder).glob("Streaming_History_Audio_*.json"))

def parse_played_at(played_at_str):
    """Format json time in datetime"""
    return datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))

def update_listening_event(cur, played_at, album_name, artist_name):
    """Updated album_name and artist_name in listening_events by played_at"""
    cur.execute("""
        UPDATE listening_events 
        SET album_name = %s, artist_name = %s
        WHERE played_at = %s AND (album_name IS NULL OR album_name = '')
    """, (album_name, artist_name, played_at))
    return cur.rowcount

def process_json_file(cur, file_path):
    """Process ine JSON-file and return number of updated events"""
    updated = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        tracks = json.load(f)
    
    for track in tracks:
        played_at_str = track.get("ts")
        if not played_at_str:
            continue
        
        album_name = track.get("master_metadata_album_album_name")
        artist_name = track.get("master_metadata_album_artist_name")
        
        if album_name and artist_name:
            played_at = parse_played_at(played_at_str)
            updated += update_listening_event(cur, played_at, album_name, artist_name)
    
    return updated

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    json_files = get_json_files()
    print(f"JSON files found: {len(json_files)}")
    
    total_updated = 0
    for file_path in json_files:
        print(f"Processing: {file_path.name}")
        updated = process_json_file(cur, file_path)
        total_updated += updated
        print(f"  Updated: {updated}")
    
    conn.commit()
    print(f"\nEvents updated: {total_updated}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()