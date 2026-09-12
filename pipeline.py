import sys
import time
from pathlib import Path
from datetime import datetime
import json
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os

from db import get_connection, get_or_create_artist, get_or_create_album, event_exists, insert_event

load_dotenv()

DATA_FOLDER = "my_spotify_data_2/Spotify Extended Streaming History"


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


def step_import_json(conn):
    """Импорт истории прослушиваний из JSON файлов Spotify."""
    json_files = sorted(Path(DATA_FOLDER).glob("Streaming_History_Audio_*.json"))
    if not json_files:
        print("[import] JSON файлы не найдены")
        return

    total_inserted = 0
    total_skipped = 0

    for file_path in json_files:
        print(f"[import] {file_path.name}...")
        with open(file_path, 'r', encoding='utf-8') as f:
            tracks = json.load(f)

        for track in tracks:
            if track.get("episode_name") is not None:
                continue

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

            artist_id = get_or_create_artist(conn, artist_name)
            album_id = get_or_create_album(conn, artist_id, album_name)
            inserted = insert_event(conn, played_at, track_id, track_name, album_id)
            total_inserted += inserted
            total_skipped += 1 - inserted

    print(f"[import] +{total_inserted} новых, {total_skipped} дубликатов")


def step_collect_spotify(conn):
    """Сбор последних 50 прослушанных треков из Spotify API."""
    try:
        sp = get_spotify_client()
    except Exception as e:
        print(f"[collect] Ошибка Spotify авторизации: {e}")
        return

    results = sp.current_user_recently_played(limit=50)
    saved = 0

    for item in results['items']:
        track = item['track']
        track_id = track['id']
        track_name = track['name']
        artist_name = track['artists'][0]['name']
        played_at = datetime.fromisoformat(item['played_at'].replace('Z', '+00:00'))

        album = track['album']
        spotify_album_id = album['id']
        album_name = album['name']
        total_tracks = album['total_tracks']
        release_date = normalize_release_date(album['release_date'])

        artist_id = get_or_create_artist(conn, artist_name)
        album_id = get_or_create_album(conn, artist_id, album_name, spotify_album_id, total_tracks, release_date)
        saved += insert_event(conn, played_at, track_id, track_name, album_id)

    print(f"[collect] +{saved} новых из Spotify API (в ответе: {len(results['items'])})")


def step_enrich_spotify(conn):
    """Обогащение альбомов данными из Spotify API (дата, кол-во треков)."""
    try:
        sp = get_spotify_client()
    except Exception as e:
        print(f"[enrich-spotify] Ошибка Spotify авторизации: {e}")
        return

    cur = conn.cursor()
    cur.execute("""
        SELECT album_id, spotify_album_id
        FROM albums
        WHERE spotify_release_date IS NULL AND spotify_total_tracks IS NULL
        LIMIT 100
    """)
    albums = cur.fetchall()

    if not albums:
        print("[enrich-spotify] Все альбомы уже обогащены")
        cur.close()
        return

    print(f"[enrich-spotify] Обработка {len(albums)} альбомов...")
    updated = 0

    for album_id, spotify_album_id in albums:
        try:
            if spotify_album_id is None:
                cur2 = conn.cursor()
                cur2.execute("SELECT track_id FROM listening_events WHERE album_id = %s LIMIT 1", (album_id,))
                track = cur2.fetchone()
                cur2.close()
                if track:
                    track_info = sp.track(track[0])
                    spotify_album_id = track_info['album']['id']
                else:
                    continue

            album_info = sp.album(spotify_album_id)
            release_date = normalize_release_date(album_info['release_date'])
            total_tracks = album_info['total_tracks']

            if release_date is None:
                continue

            cur.execute("""
                UPDATE albums SET spotify_release_date = %s, spotify_total_tracks = %s WHERE album_id = %s
            """, (release_date, total_tracks, album_id))
            conn.commit()
            updated += 1
        except Exception as e:
            conn.rollback()

        time.sleep(0.5)

    cur.close()
    print(f"[enrich-spotify] Обновлено: {updated}")


def step_enrich_musicbrainz(conn):
    """Обогащение данных из MusicBrainz (mbid, треки, даты, алиасы)."""
    try:
        import musicbrainzngs
        musicbrainzngs.set_useragent("MusicDiary", "1.0", "user@example.com")
    except ImportError:
        print("[enrich-mb] musicbrainzngs не установлен, пропуск")
        return

    cur = conn.cursor()
    cur.execute("""
        SELECT a.album_id, a.album_name, ar.artist_name, ar.artist_id
        FROM albums a
        JOIN artists ar ON a.artist_id = ar.artist_id
        WHERE a.total_tracks IS NULL AND a.album_name IS NOT NULL
        LIMIT 200
    """)
    albums = cur.fetchall()

    if not albums:
        print("[enrich-mb] Все альбомы уже обогащены")
        cur.close()
        return

    print(f"[enrich-mb] Обработка {len(albums)} альбомов...")
    updated = 0

    for album_id, album_name, artist_name, artist_id in albums:
        try:
            result = musicbrainzngs.search_releases(
                query=f'artist:"{artist_name}" AND release:"{album_name}"',
                limit=1, strict=True
            )
            releases = result.get('release-list', [])

            if not releases:
                result = musicbrainzngs.search_releases(
                    query=f'artist:{artist_name} AND release:{album_name}',
                    limit=1, strict=False
                )
                releases = result.get('release-list', [])

            if not releases:
                continue

            release = releases[0]
            mbid = release['id']
            release_info = musicbrainzngs.get_release_by_id(mbid, includes=['recordings', 'artist-credits'])
            release_data = release_info['release']
            total_tracks = len(release_data.get('medium-list', [{}])[0].get('track-list', []))
            release_date = release_data.get('date')

            if release_date and len(release_date) == 4:
                release_date = f"{release_date}-01-01"
            elif release_date and len(release_date) == 7:
                release_date = f"{release_date}-01"

            cur.execute("UPDATE albums SET mbid = %s, total_tracks = %s, release_date = %s WHERE album_id = %s",
                        (mbid, total_tracks, release_date, album_id))

            artist_mbid = release_data['artist-credit'][0]['artist']['id']
            artist_info = musicbrainzngs.get_artist_by_id(artist_mbid)
            artist_aliases = []
            for alias in artist_info['artist'].get('alias-list', []):
                if alias.get('locale') == 'ru':
                    artist_aliases.append(alias['alias'])

            cur.execute("UPDATE artists SET mbid = %s, artist_name = %s, artist_alias = %s WHERE artist_id = %s",
                        (artist_mbid, artist_info['artist']['name'],
                         ', '.join(artist_aliases) if artist_aliases else None, artist_id))

            conn.commit()
            updated += 1
        except Exception as e:
            conn.rollback()

        time.sleep(0.5)

    cur.close()
    print(f"[enrich-mb] Обновлено: {updated}")


def step_export_csv(conn):
    """Экспорт полностью прослушанных альбомов в CSV."""
    import pandas as pd

    query = """
        WITH track_plays AS (
            SELECT 
                a.album_name, ar.artist_name,
                a.spotify_release_date, a.spotify_total_tracks,
                le.track_id, COUNT(*) as track_play_count
            FROM listening_events le
            JOIN albums a ON le.album_id = a.album_id
            JOIN artists ar ON a.artist_id = ar.artist_id
            WHERE a.spotify_release_date IS NOT NULL 
              AND a.spotify_total_tracks IS NOT NULL
            GROUP BY a.album_name, ar.artist_name, a.spotify_release_date, 
                     a.spotify_total_tracks, le.track_id
        ),
        album_full_plays AS (
            SELECT 
                album_name, artist_name,
                spotify_release_date, spotify_total_tracks,
                COUNT(DISTINCT track_id) as listened_tracks,
                MIN(track_play_count) as full_album_plays
            FROM track_plays
            GROUP BY album_name, artist_name, spotify_release_date, spotify_total_tracks
            HAVING COUNT(DISTINCT track_id) = spotify_total_tracks
        )
        SELECT 
            album_name, artist_name, full_album_plays,
            EXTRACT(YEAR FROM spotify_release_date) as release_year,
            spotify_total_tracks as total_tracks
        FROM album_full_plays
        WHERE spotify_total_tracks >= 3
        ORDER BY release_year, artist_name
    """

    df = pd.read_sql(query, conn)
    df.to_csv("album_full_plays.csv", index=False, encoding='utf-8')
    print(f"[export] {len(df)} альбомов → album_full_plays.csv")
    return df


def step_generate_dashboard():
    """Генерация HTML дашборда из CSV."""
    import pandas as pd
    import json

    df = pd.read_csv("album_full_plays.csv", encoding="utf-8")
    df["release_year"] = df["release_year"].astype(int)
    df["full_album_plays"] = df["full_album_plays"].astype(int)

    years = sorted(int(y) for y in df["release_year"].unique())
    plays_by_year = df.groupby("release_year")["full_album_plays"].sum().to_dict()
    artist_totals = df.groupby("artist_name")["full_album_plays"].sum().to_dict()
    artists = sorted(artist_totals, key=lambda a: (-artist_totals[a], a.lower()))

    artist_year = {}
    artist_year_albums = {}
    for a in artists:
        sub = df[df["artist_name"] == a]
        per_year = sub.groupby("release_year")["full_album_plays"].sum()
        artist_year[a] = {int(y): int(v) for y, v in per_year.items()}
        d = {}
        for y, g in sub.groupby("release_year"):
            d[int(y)] = [{"name": r.album_name, "plays": int(r.full_album_plays)} for r in g.itertuples()]
        artist_year_albums[a] = d

    data = {
        "years": years,
        "plays_by_year": {int(k): int(v) for k, v in plays_by_year.items()},
        "artists": artists,
        "artist_year": artist_year,
        "artist_year_albums": artist_year_albums,
        "artist_totals": {a: int(artist_totals[a]) for a in artists},
    }

    with open("album_dashboard_template.html", "r", encoding="utf-8") as tf:
        template = tf.read()

    html = template.replace("__DATA__", json.dumps(data, ensure_ascii=False))

    with open("album_dashboard.html", "w", encoding="utf-8") as f:
        f.write(html)

    pages_dir = os.path.join("github-pages")
    os.makedirs(pages_dir, exist_ok=True)
    with open(os.path.join(pages_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[dashboard] {len(artists)} артистов, {len(years)} лет → album_dashboard.html")


def main():
    steps = {
        "import": step_import_json,
        "collect": step_collect_spotify,
        "enrich-spotify": step_enrich_spotify,
        "enrich-mb": step_enrich_musicbrainz,
        "export": step_export_csv,
        "dashboard": step_generate_dashboard,
    }

    if len(sys.argv) > 1:
        run_steps = sys.argv[1:]
    else:
        run_steps = list(steps.keys())

    conn = get_connection()

    for step_name in run_steps:
        if step_name not in steps:
            print(f"Неизвестный шаг: {step_name}")
            continue
        steps[step_name](conn)

    conn.close()
    print("\nГотово!")


if __name__ == "__main__":
    main()
