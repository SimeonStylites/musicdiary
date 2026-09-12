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

def normalize_release_date(date_str):
    if not date_str:
        return None
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str
    if len(date_str) == 7 and date_str[4] == '-':
        return f"{date_str}-01"
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return None


def main():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()
    sp = get_spotify_client()

    #1.Finding albums withut data
    cur.execute("""
        SELECT album_id, spotify_album_id
        FROM albums
        WHERE spotify_release_date IS NULL AND spotify_total_tracks IS NULL
        LIMIT 100
    """)
    albums = cur.fetchall()
    print(f"Albums without data: {len(albums)}")

    for album_id, spotify_album_id in albums:
        try:
            # 2. If no spotify_album_id take it from track
            if spotify_album_id is None:
                cur.execute("""
                    SELECT le.track_id
                    FROM listening_events le
                    WHERE le.album_id = %s
                    LIMIT 1
                """, (album_id,))
                track = cur.fetchone()
                if track:
                    track_info = sp.track(track[0])
                    spotify_album_id = track_info['album']['id']
                    print(f"Got spotify_album_id: {spotify_album_id}")
                else:
                    print(f"No tracks found for {album_id}")
                    continue

            #3.Getting data from Spotify
            album_info = sp.album(spotify_album_id)
            release_date_raw = album_info['release_date']
            release_date = normalize_release_date(release_date_raw)
            total_tracks = album_info['total_tracks']

            if release_date is None:
                print(f"Couldn't normalize date: {release_date_raw} for album {album_id}")
                continue

            #4.Update
            cur.execute("""
                UPDATE albums
                SET spotify_release_date = %s, spotify_total_tracks = %s
                WHERE album_id = %s
            """, (release_date, total_tracks, album_id))
            conn.commit()
            print(f"Album {album_id} is updated: {release_date}, {total_tracks} tracks")

        except Exception as e:
            print(f"Error for album {album_id}: {e}")
            conn.rollback()

        time.sleep(0.5)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()