import json
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

DATA_FOLDER = "my_spotify_data_2/Spotify Extended Streaming History"
json_files = sorted(Path(DATA_FOLDER).glob("Streaming_History_Audio_*.json"))
print(f"Найдено файлов: {len(json_files)}")

def get_or_create_artist(conn, artist_name):
    """Возвращает artist_id. Если исполнителя нет — создаёт."""
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

def get_or_create_album(conn, artist_id, album_name, spotify_album_id=None):
    """Возвращает album_id. Если альбома нет — создаёт."""
    cur = conn.cursor()
    cur.execute("""
        SELECT album_id FROM albums 
        WHERE artist_id = %s AND album_name = %s
    """, (artist_id, album_name))
    row = cur.fetchone()
    if row:
        cur.close()
        return row[0]
    
    cur.execute("""
        INSERT INTO albums (artist_id, album_name, spotify_album_id)
        VALUES (%s, %s, %s)
        RETURNING album_id
    """, (artist_id, album_name, spotify_album_id))
    album_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return album_id

total_inserted = 0
total_skipped = 0

for file_path in json_files:
    print(f"Обработка: {file_path.name}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        tracks = json.load(f)
    
    for track in tracks:
        # Пропускаем подкасты
        if track.get("episode_name") is not None:
            continue
        
        # Извлекаем данные
        played_at_str = track.get("ts")
        if not played_at_str:
            continue
            
        played_at = datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))
        
        track_uri = track.get("spotify_track_uri", "")
        track_id = track_uri.split(":")[-1] if track_uri else None
        
        track_name = track.get("master_metadata_track_name")
        artist_name = track.get("master_metadata_album_artist_name")
        album_name = track.get("master_metadata_album_album_name")
        
        if not track_name or not artist_name or not album_name:
            continue
        
        # Получаем artist_id
        artist_id = get_or_create_artist(conn, artist_name)
        
        # Получаем album_id (spotify_album_id пока нет, но можно оставить None)
        album_id = get_or_create_album(conn, artist_id, album_name, spotify_album_id=None)
        
        # Вставляем событие
        cur.execute("""
            INSERT INTO listening_events (played_at, track_id, track_name, album_id, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (played_at) DO NOTHING
        """, (played_at, track_id, track_name, album_id))
        
        if cur.rowcount > 0:
            total_inserted += 1
        else:
            total_skipped += 1

conn.commit()
cur.close()
conn.close()

print(f"\nДобавлено: {total_inserted}")
print(f"Пропущено (дубликаты): {total_skipped}")