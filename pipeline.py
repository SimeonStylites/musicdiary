import sys
import time
import traceback
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

DATA_FOLDER = "my_spotify_data_3/Spotify Extended Streaming History"
LOG_FILE = "pipeline.log"


def log(message):
    """Дублирует print в лог-файл с временной меткой."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}\n")


def _init_logging():
    """Переводит stdout в UTF-8 и пишет каждое сообщение ещё и в лог-файл."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    console_print = print

    def _print(*args, **kwargs):
        if args:
            log(kwargs.get("sep", " ").join(str(a) for a in args))
        return console_print(*args, **kwargs)

    return _print


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
    fill_albums_from_parquet(conn)


def fill_albums_from_parquet(conn):
    """Заполняет spotify_album_id/дату/tt в albums из общего parquet — без API.
    Мост: события БД -> tracks_min -> albums_min. Для альбомов, у которых sid ещё нет.
    Недостающее остаётся на enrich-spotify."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT le.album_id, le.track_id
        FROM listening_events le
        JOIN albums a ON a.album_id = le.album_id
        WHERE le.track_id IS NOT NULL AND a.spotify_album_id IS NULL
    """)
    pairs = cur.fetchall()
    cur.close()
    if not pairs:
        print("[import] все альбомы уже с sid из parquet")
        return
    n_albums = len({a for a, _ in pairs})

    import duckdb
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    con.execute("CREATE TABLE want(album_id INTEGER, track_id VARCHAR)")
    con.executemany("INSERT INTO want VALUES (?,?)", pairs)
    tr_dir = TRACKS_MIN_DIR.replace(chr(92), "/")
    al_dir = ALBUMS_MIN_DIR.replace(chr(92), "/")

    # мост пишем пошагово: одним тройным join duckdb разливается (проверено: 412 с)
    t0 = time.time()
    con.execute(f"""
        CREATE TABLE bridge AS
        SELECT w.album_id, w.track_id, t.album_id AS sid
        FROM want w JOIN read_parquet('{tr_dir}/*.parquet') t ON t.id = w.track_id
    """)
    con.execute(f"""
        CREATE TABLE joined AS
        SELECT b.album_id, b.sid, m.release_date, m.total_tracks,
               count(DISTINCT b.track_id) AS my_n
        FROM bridge b
        JOIN read_parquet('{al_dir}/*.parquet') m ON m.id = b.sid
        GROUP BY 1, 2, 3, 4
    """)
    # правило выбора издания: самая ранняя дата | больше моих треков | sid по алфавиту
    # (sid в БД здесь не участвует — выборка только по альбомам, где его нет)
    con.execute("""
        CREATE TABLE pick AS
        SELECT album_id,
               arg_min(sid, coalesce(cast(release_date AS VARCHAR), '9999-99-99') || '|' ||
                           lpad(cast(my_n AS VARCHAR), 5, '0') || '|' || sid) AS sid
        FROM joined GROUP BY 1
    """)
    rows = con.execute("""
        SELECT p.album_id, p.sid, c.release_date, c.total_tracks
        FROM pick p JOIN joined c ON c.album_id = p.album_id AND c.sid = p.sid
    """).fetchall()
    con.close()

    if not rows:
        print(f"[import] в parquet нет ни одного трека из {n_albums} альбомов, "
              f"оставлено на enrich-spotify ({round(time.time()-t0,1)} с)")
        return

    def norm_date(d):
        if d and str(d).startswith("0000"):
            return None
        return normalize_release_date(d)

    cur = conn.cursor()
    updated = 0
    for album_id, sid, raw_date, tt in rows:
        sets = ["spotify_album_id = %s"]
        params = [sid]
        d = norm_date(raw_date)
        if d:
            sets.append("spotify_release_date = %s")
            params.append(d)
        if tt is not None:
            sets.append("spotify_total_tracks = %s")
            params.append(int(tt))
        params.append(album_id)
        cur.execute(f"UPDATE albums SET {', '.join(sets)} "
                    f"WHERE album_id = %s AND spotify_album_id IS NULL", params)
        updated += cur.rowcount
    conn.commit()
    cur.close()
    print(f"[import] sid из parquet: {updated} альбомов из {n_albums} "
          f"(остальные — на enrich-spotify), {round(time.time()-t0,1)} с")


def step_collect_spotify(conn):
    """Сбор последних 50 прослушанных треков из Spotify API."""
    try:
        sp = get_spotify_client()
    except Exception as e:
        print(f"[collect] Ошибка Spotify авторизации: {e}")
        return

    try:
        results = sp.current_user_recently_played(limit=50)
    except Exception as e:
        print(f"[collect] Ошибка Spotify API: {e}")
        return

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


ALBUMS_MIN_DIR = r"E:\spotify_parquet\albums_min"


def sync_albums_min(conn):
    """Дописывает в свой parquet альбомы, которых там нет (sid из БД)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT a.spotify_album_id, a.spotify_release_date, a.spotify_total_tracks
        FROM albums a
        WHERE a.spotify_album_id IS NOT NULL
          AND a.spotify_release_date IS NOT NULL
          AND a.spotify_total_tracks IS NOT NULL
          AND EXISTS (SELECT 1 FROM listening_events le WHERE le.album_id = a.album_id)
    """)
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print("[sync-albums] в БД нет альбомов с sid")
        return

    import duckdb
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    con.execute("CREATE TABLE want(sid VARCHAR, rd DATE, tt INTEGER)")
    con.executemany("INSERT INTO want VALUES (?,?,?)", rows)

    dir_sql = ALBUMS_MIN_DIR.replace(chr(92), "/")
    missing = con.execute(f"""
        SELECT sid, rd, tt FROM want w
        WHERE NOT EXISTS (SELECT 1 FROM read_parquet('{dir_sql}/*.parquet') p WHERE p.id = w.sid)
    """).fetchall()

    if not missing:
        print(f"[sync-albums] всё на месте ({len(rows)} альбомов в БД)")
        con.close()
        return

    con.execute("""CREATE TABLE delta(
        id VARCHAR, release_date DATE, release_date_precision VARCHAR, total_tracks INTEGER)""")
    con.executemany("INSERT INTO delta VALUES (?,?,NULL,?)",
                    [(s, d, t) for s, d, t in missing])
    out = f"{dir_sql}/delta-{datetime.now():%Y%m%d-%H%M%S}.parquet"
    con.execute(f"COPY delta TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    con.close()
    print(f"[sync-albums] дописано в parquet: {len(missing)} (всего в БД: {len(rows)})")


TRACKS_MIN_DIR = r"E:\spotify_parquet\tracks_min"


def _json_track_ids(folders):
    """Все spotify_track_uri из JSON-файлов (свои и чужие)."""
    ids = set()
    for folder in folders:
        for f in sorted(Path(folder).glob("*.json")):
            try:
                events = json.loads(f.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"[sync-tracks] не прочитал {f.name}: {e}")
                continue
            for e in events:
                uri = e.get("spotify_track_uri")
                if uri:
                    ids.add(uri.split(":")[-1])
    return ids


def sync_tracks_min(conn, sp, json_folders=None):
    """Дописывает в tracks_min связи трек→альбом для непокрытых треков
    (источники: listening_events + JSON) и при необходимости — альбомы в albums_min."""
    folders = json_folders or [DATA_FOLDER]

    cur = conn.cursor()
    cur.execute("SELECT DISTINCT track_id FROM listening_events WHERE track_id IS NOT NULL")
    ids = {r[0] for r in cur.fetchall()}
    cur.close()
    n_db = len(ids)
    ids |= _json_track_ids(folders)

    import duckdb
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    con.execute("CREATE TABLE want(id VARCHAR)")
    con.executemany("INSERT INTO want VALUES (?)", [(i,) for i in ids])
    tr_dir = TRACKS_MIN_DIR.replace(chr(92), "/")
    missing = [r[0] for r in con.execute(f"""
        SELECT w.id FROM want w
        WHERE NOT EXISTS (SELECT 1 FROM read_parquet('{tr_dir}/*.parquet') p WHERE p.id = w.id)
    """).fetchall()]

    if not missing:
        print(f"[sync-tracks] всё на месте ({len(ids)} треков: {n_db} из БД, "
              f"{len(ids) - n_db} из JSON)")
        con.close()
        return

    print(f"[sync-tracks] непокрытых треков: {len(missing)}, спрашиваю API...")
    links, album_ids, errors = [], set(), 0
    for tid in missing:
        try:
            aid = sp.track(tid)["album"]["id"]
            links.append((tid, aid))
            album_ids.add(aid)
        except Exception:
            errors += 1
        time.sleep(0.5)

    con.execute("CREATE TABLE aid(id VARCHAR)")
    con.executemany("INSERT INTO aid VALUES (?)", [(a,) for a in album_ids])
    al_dir = ALBUMS_MIN_DIR.replace(chr(92), "/")
    need_album = [r[0] for r in con.execute(f"""
        SELECT a.id FROM aid a
        WHERE NOT EXISTS (SELECT 1 FROM read_parquet('{al_dir}/*.parquet') p WHERE p.id = a.id)
    """).fetchall()]

    new_albums = []
    for aid in need_album:
        try:
            info = sp.album(aid)
            raw = info.get("release_date") or ""
            rd = None if raw.startswith("0000") else normalize_release_date(raw)
            if rd is None:
                print(f"[sync-tracks] у {aid} нет даты релиза ({raw!r}), пишу без неё")
            new_albums.append((aid, rd, info.get("release_date_precision"),
                               info.get("total_tracks")))
        except Exception:
            errors += 1
        time.sleep(0.5)

    if links:
        con.execute("CREATE TABLE tl(id VARCHAR, album_id VARCHAR)")
        con.executemany("INSERT INTO tl VALUES (?,?)", links)
        out = f"{tr_dir}/delta-{datetime.now():%Y%m%d-%H%M%S}.parquet"
        con.execute(f"COPY tl TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        print(f"[sync-tracks] дописано связей: {len(links)}")

    if new_albums:
        con.execute("""CREATE TABLE na(
            id VARCHAR, release_date DATE, release_date_precision VARCHAR, total_tracks INTEGER)""")
        con.executemany("INSERT INTO na VALUES (?,?,?,?)", new_albums)
        out = f"{al_dir}/delta-{datetime.now():%Y%m%d-%H%M%S}.parquet"
        con.execute(f"COPY na TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        print(f"[sync-tracks] дописано альбомов: {len(new_albums)}")

    con.close()
    print(f"[sync-tracks] треков проверено: {len(missing)}, без ответа API: {errors}")


def step_enrich_spotify(conn):
    """Обогащение альбомов данными из Spotify API (дата, кол-во треков)."""
    try:
        sp = get_spotify_client()
    except Exception as e:
        print(f"[enrich-spotify] Ошибка Spotify авторизации: {e}")
        return

    cur = conn.cursor()
    cur.execute("""
        SELECT a.album_id, a.spotify_album_id
        FROM albums a
        WHERE (a.spotify_album_id IS NULL
               OR a.spotify_release_date IS NULL OR a.spotify_total_tracks IS NULL)
          AND EXISTS (SELECT 1 FROM listening_events le WHERE le.album_id = a.album_id)
        LIMIT 100
    """)
    albums = cur.fetchall()

    if not albums:
        print("[enrich-spotify] Все альбомы уже обогащены")
        cur.close()
        sync_albums_min(conn)
        sync_tracks_min(conn, sp)
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
                UPDATE albums SET spotify_album_id = %s, spotify_release_date = %s,
                                  spotify_total_tracks = %s WHERE album_id = %s
            """, (spotify_album_id, release_date, total_tracks, album_id))
            conn.commit()
            updated += 1
        except Exception as e:
            conn.rollback()

        time.sleep(0.5)

    cur.close()
    print(f"[enrich-spotify] Обновлено: {updated}")
    sync_albums_min(conn)
    sync_tracks_min(conn, sp)


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


def step_generate_dashboard(conn=None):
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

    pages_dir = os.path.join("docs")
    os.makedirs(pages_dir, exist_ok=True)
    with open(os.path.join(pages_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[dashboard] {len(artists)} артистов, {len(years)} лет → album_dashboard.html")


def step_push(conn=None):
    """Коммит изменённых файлов и отправка в origin."""
    import subprocess

    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GCM_INTERACTIVE"] = "Never"

    def run(*args):
        return subprocess.run(["git", *args], capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)

    status = run("status", "--porcelain")
    if status.returncode != 0:
        print(f"[push] git status не отработал: {status.stderr.strip()}")
        return

    if not status.stdout.strip():
        print("[push] Изменений нет, пропуск")
        return

    add = run("add", "album_full_plays.csv", "album_dashboard.html", "docs/index.html")
    if add.returncode != 0:
        print(f"[push] git add не отработал: {add.stderr.strip()}")
        return

    staged = run("diff", "--cached", "--quiet")
    if staged.returncode == 0:
        print("[push] Изменений нет, пропуск")
        return

    commit = run("commit", "-m", "Auto-update dashboard")
    if commit.returncode != 0:
        print(f"[push] git commit не отработал: {commit.stderr.strip()}")
        return
    print(f"[push] {commit.stdout.strip()}")

    push = run("push")
    if push.returncode != 0:
        print(f"[push] git push не отработал: {push.stderr.strip()}")
        return
    print(f"[push] {push.stdout.strip()}")


def main():
    steps = {
        "import": step_import_json,
        "collect": step_collect_spotify,
        "enrich-spotify": step_enrich_spotify,
        "enrich-mb": step_enrich_musicbrainz,
        "export": step_export_csv,
        "dashboard": step_generate_dashboard,
        "push": step_push,
    }

    if len(sys.argv) > 1:
        run_steps = sys.argv[1:]
    else:
        run_steps = list(steps.keys())

    globals()["print"] = _init_logging()
    log(f"=== Запуск: {' '.join(run_steps)} ===")

    conn = get_connection()

    for step_name in run_steps:
        if step_name not in steps:
            print(f"Неизвестный шаг: {step_name}")
            continue
        try:
            steps[step_name](conn)
        except Exception:
            print(f"[{step_name}] Шаг упал:\n{traceback.format_exc()}")

    conn.close()
    print("\nГотово!")


if __name__ == "__main__":
    main()
