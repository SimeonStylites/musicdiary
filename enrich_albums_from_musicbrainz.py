import psycopg2
import musicbrainzngs
from dotenv import load_dotenv
import os
import time

load_dotenv()

musicbrainzngs.set_useragent("MusicDiary", "1.0", "your_email@example.com")

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_albums_without_details(conn, limit=100):
    cur = conn.cursor()
    cur.execute("""
        SELECT a.album_name, ar.artist_name, a.album_id, ar.artist_id
        FROM albums a
        JOIN artists ar ON a.artist_id = ar.artist_id
        WHERE a.total_tracks IS NULL AND a.album_name IS NOT NULL
        LIMIT %s
    """, (limit,))
    albums = cur.fetchall()
    cur.close()
    return albums

def search_album_strict(artist_name, album_name):
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
            release_info = musicbrainzngs.get_release_by_id(mbid, includes=['recordings', 'artist-credits'])
            release_data = release_info['release']
            total_tracks = len(release_data.get('medium-list', [{}])[0].get('track-list', []))
            release_date = release_data.get('date', None)
            
            artist_mbid = release_data['artist-credit'][0]['artist']['id']
            artist_info = musicbrainzngs.get_artist_by_id(artist_mbid)
            artist_name_mb = artist_info['artist']['name']
            
            return mbid, total_tracks, release_date, artist_mbid, artist_name_mb
    except Exception as e:
        print(f"  Strict search error: {e}")
    return None, None, None, None, None

def search_album_fuzzy(artist_name, album_name):
    try:
        result = musicbrainzngs.search_releases(
            query=f'artist:{artist_name} AND release:{album_name}',
            limit=1,
            strict=False
        )
        releases = result.get('release-list', [])
        if releases:
            release = releases[0]
            mbid = release['id']
            release_info = musicbrainzngs.get_release_by_id(mbid, includes=['recordings', 'artist-credits'])
            release_data = release_info['release']
            total_tracks = len(release_data.get('medium-list', [{}])[0].get('track-list', []))
            release_date = release_data.get('date', None)
            
            artist_mbid = release_data['artist-credit'][0]['artist']['id']
            artist_info = musicbrainzngs.get_artist_by_id(artist_mbid)
            artist_name_mb = artist_info['artist']['name']
            
            artist_aliases = []
            if 'alias-list' in artist_info['artist']:
                for alias in artist_info['artist']['alias-list']:
                    if alias.get('locale') == 'ru':
                        artist_aliases.append(alias['alias'])
            
            album_aliases = []
            if 'alias-list' in release_data:
                for alias in release_data['alias-list']:
                    if alias.get('locale') == 'ru':
                        album_aliases.append(alias['alias'])
            
            return mbid, total_tracks, release_date, artist_mbid, artist_name_mb, artist_aliases, album_aliases
    except Exception as e:
        print(f"  Fuzzy search error: {e}")
    return None, None, None, None, None, [], []

def update_album_info(conn, album_id, artist_id, mbid, total_tracks, release_date, artist_mbid, artist_name_mb, artist_aliases, album_aliases):
    cur = conn.cursor()
    
    if release_date and len(release_date) == 4:
        release_date = f"{release_date}-01-01"
    elif release_date and len(release_date) == 7:
        release_date = f"{release_date}-01"
    
    #Update album
    cur.execute("""
        UPDATE albums
        SET mbid = %s, total_tracks = %s, release_date = %s
        WHERE album_id = %s
    """, (mbid, total_tracks, release_date, album_id))
    
    #Update artist
    cur.execute("""
        UPDATE artists
        SET mbid = %s, artist_name = %s, artist_alias = %s
        WHERE artist_id = %s
    """, (artist_mbid, artist_name_mb, ', '.join(artist_aliases) if artist_aliases else None, artist_id))
    
    #Update album with aliases
    if album_aliases:
        cur.execute("""
            UPDATE albums
            SET album_alias = %s
            WHERE album_id = %s
        """, (', '.join(album_aliases), album_id))
    
    conn.commit()
    cur.close()

def main():
    conn = get_connection()
    albums = get_albums_without_details(conn, limit=200)
    print(f"Albums found for processing: {len(albums)}")
    
    updated = 0
    for album_name, artist_name, album_id, artist_id in albums:
        print(f"\nProcessing: {artist_name} - {album_name}")
        
        mbid, total_tracks, release_date, artist_mbid, artist_name_mb = search_album_strict(artist_name, album_name)
        
        if mbid:
            update_album_info(conn, album_id, artist_id, mbid, total_tracks, release_date, artist_mbid, artist_name_mb, [], [])
            updated += 1
            print(f"Found (strict search): {total_tracks} tracks")
        else:
            print(f"Trying fuzzy search...")
            result = search_album_fuzzy(artist_name, album_name)
            if result[0]:
                mbid, total_tracks, release_date, artist_mbid, artist_name_mb, artist_aliases, album_aliases = result
                update_album_info(conn, album_id, artist_id, mbid, total_tracks, release_date, artist_mbid, artist_name_mb, artist_aliases, album_aliases)
                updated += 1
                print(f"Fuzzy search: {total_tracks} tracks")
                if artist_aliases:
                    print(f"Artist aliases: {', '.join(artist_aliases)}")
                if album_aliases:
                    print(f"Album aliases: {', '.join(album_aliases)}")
            else:
                print(f"Not found")
        
        time.sleep(0.5)
    
    print(f"\n Albums: {updated}")
    conn.close()

if __name__ == "__main__":
    main()