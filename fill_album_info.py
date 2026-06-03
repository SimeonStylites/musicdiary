import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import time

load_dotenv()

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
        scope="user-read-recently-played"
    ))

def get_track_ids_without_album(conn, limit=10000):
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
        WHERE track_id = %s
    """, (album_id, track_id))
    conn.commit()
    cur.close()

def save_album(conn, album_info):
    cur = conn.cursor()
    album_id = album_info['id']
    album_name = album_info['name']
    artist_name = album_info['artists'][0]['name']
    total_tracks = album_info['total_tracks']
    release_date = album_info['release_date']
    
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

def main():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    sp = get_spotify_client()
    
    tracks = get_track_ids_without_album(conn, limit=7000)
    print(f"Найдено треков без album_id: {len(tracks)}")
    
    total_updated = 0
    # Обрабатываем пачками по 50 треков
    for i in range(0, len(tracks), 50):
        batch = tracks[i:i+50]
        batch_ids = [track[0] for track in batch if track[0]]
        
        try:
            # Массовый запрос к API
            tracks_info = sp.tracks(batch_ids)
            
            for j, track_info in enumerate(tracks_info['tracks']):
                if track_info:
                    track_id = batch[j][0]
                    album = track_info['album']
                    album_id = album['id']
                    
                    # Сохраняем альбом
                    save_album(conn, album)
                    # Обновляем трек
                    update_track_album_id(conn, track_id, album_id)
                    total_updated += 1
                    
                    print(f"✅ {batch[j][1]} - {batch[j][2]} → альбом: {album['name']}")
                else:
                    print(f"⚠️ Не найден трек: {batch[j][1]}")
            
            time.sleep(0.3)  # Пауза между пачками
            
        except Exception as e:
            print(f"❌ Ошибка в пачке {i//50 + 1}: {e}")
            time.sleep(2)
            continue
    
    print(f"\n✅ Обновлено треков: {total_updated}")
    
    # Финальная проверка
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM listening_events WHERE album_id IS NULL")
    remaining = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"Осталось треков без album_id: {remaining}")

if __name__ == "__main__":
    main()