import psycopg2
import musicbrainzngs
from dotenv import load_dotenv
import os
import time

load_dotenv()

musicbrainzngs.set_useragent("MusicDiary", "1.0", "klimachkovdmitry@gmail.com")

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_albums_without_details(conn, limit=50):
    cur = conn.cursor()
    cur.execute("""
        SELECT album_name, artist_name
        FROM albums
        WHERE total_tracks IS NULL AND album_name IS NOT NULL
        LIMIT %s
    """, (limit,))
    albums = cur.fetchall()
    cur.close()
    return albums

def search_album_in_musicbrainz(artist_name, album_name):
    try:
        result = musicbrainzngs.search_releases(
            query=f'artist:"{artist_name}" AND release:"{album_name}"',
            limit=1,
            strict=True
        )
        releases = result.get('release-list', [])
        if releases:
            release = releases[0]
            mbid = release['id']
            #Getting details for total_tracks
            release_info = musicbrainzngs.get_release_by_id(mbid, includes=['recordings'])
            release_data = release_info['release']
            total_tracks = len(release_data.get('medium-list', [{}])[0].get('track-list', []))
            release_date = release_data.get('date', None)
            return mbid, total_tracks, release_date
    except Exception as e:
        print(f"MusicBrainz error: {e}")
    return None, None, None

def update_album_info(conn, album_name, artist_name, mbid, total_tracks, release_date):
    cur = conn.cursor()
    if release_date and len(release_date) == 4:
        release_date = f"{release_date}-01-01"
    elif release_date and len(release_date) == 7:
        release_date = f"{release_date}-01"
    
    cur.execute("""
        UPDATE albums
        SET mbid = %s, total_tracks = %s, release_date = %s
        WHERE album_name = %s AND artist_name = %s
    """, (mbid, total_tracks, release_date, album_name, artist_name))
    conn.commit()
    cur.close()

def main():
    conn = get_connection()
    albums = get_albums_without_details(conn, limit=2000)
    print(f"Albums without details: {len(albums)}")
    
    updated = 0
    for album_name, artist_name in albums:
        print(f"Finding: {artist_name} - {album_name}")
        mbid, total_tracks, release_date = search_album_in_musicbrainz(artist_name, album_name)
        
        if mbid:
            update_album_info(conn, album_name, artist_name, mbid, total_tracks, release_date)
            updated += 1
            print(f"Mbid: {mbid}, tracks: {total_tracks}, date: {release_date}")
        else:
            print(f"Not found in MusicBrainz")
        
        time.sleep(0.5)
    
    print(f"\nAlbums updated: {updated}")
    conn.close()

if __name__ == "__main__":
    main()