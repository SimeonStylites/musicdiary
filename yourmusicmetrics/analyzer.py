import io
import json
import os
import sqlite3
import sys
import time
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TRACKS_MIN = os.environ.get("YMM_TRACKS_MIN", r"E:\spotify_parquet\tracks_min")
ALBUMS_MIN = os.environ.get("YMM_ALBUMS_MIN", r"E:\spotify_parquet\albums_min")
TRACKS_IDX = os.environ.get("YMM_TRACKS_IDX", r"E:\spotify_parquet\tracks_idx.sqlite3")
ALBUMS_IDX = os.environ.get("YMM_ALBUMS_IDX", r"E:\spotify_parquet\albums_idx.sqlite3")
TEMPLATE = PROJECT_ROOT / "album_dashboard_template.html"

MAX_UNCOMPRESSED = 512 * 1024 * 1024
MAX_FILES = 60
MAX_API_CALLS = 500
COOLDOWN_FILE = PROJECT_ROOT / "api_cooldown.json"
COOLDOWN_HOURS = 24
MIN_TRACK_PLAYS = 10
ARTIST_DYN_N = 100
TRACK_DYN_N = 25


def load_upload(upload_bytes, filename):
    return load_uploads([(upload_bytes, filename)])


def load_uploads(uploads):
    """Возвращает нормализованные события из набора ZIP/JSON (общий дедуп)."""
    raw_lists = []
    for upload_bytes, filename in uploads:
        fname = filename.lower()
        if fname.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(upload_bytes)) as z:
                names = [
                    n for n in z.namelist()
                    if "Streaming_History_Audio_" in n and n.endswith(".json")
                ][:MAX_FILES]
                total = 0
                for n in names:
                    info = z.getinfo(n)
                    total += info.file_size
                    if total > MAX_UNCOMPRESSED:
                        raise ValueError("архив слишком большой")
                    with z.open(n) as fh:
                        raw_lists.append(json.load(fh))
            if not names:
                raise ValueError("в архиве нет Streaming_History_Audio_*.json")
        elif fname.endswith(".json"):
            try:
                data = json.loads(upload_bytes.decode("utf-8"))
            except Exception:
                raise ValueError("не удалось прочитать JSON")
            if isinstance(data, list):
                raw_lists.append(data)
            else:
                raise ValueError("JSON должен быть списком событий")
        else:
            raise ValueError("принимаются только .zip или .json")

    events, seen = [], set()
    for lst in raw_lists:
        for t in lst:
            if t.get("episode_name") is not None:
                continue
            ts = t.get("ts")
            uri = t.get("spotify_track_uri", "") or ""
            if not ts or not uri.startswith("spotify:track:"):
                continue
            if not t.get("master_metadata_track_name") or \
                    not t.get("master_metadata_album_artist_name") or \
                    not t.get("master_metadata_album_album_name"):
                continue
            key = f"{ts}|{uri}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "ts": datetime.fromisoformat(ts.replace("Z", "+00:00")),
                "track_id": uri.split(":")[-1],
                "track_name": t["master_metadata_track_name"],
                "artist_name": t["master_metadata_album_artist_name"],
                "album_name": t["master_metadata_album_album_name"],
                "ms_played": int(t.get("ms_played", 0) or 0),
            })
    return events


def race_frames(events, n_frames=120):
    """Хронологический топ-10 по мере «сканирования» событий от старых к новым.

    Каждый кадр — топ-10 по накопленным прослушиваниям на этот момент времени.
    Считается только по ts/имени/артисту из JSON (мост track_id→album и Spotify
    API не нужны), поэтому готов сразу после parse и не зависит от медленного
    этапа analyze. Сервер отдаёт кадры браузеру, тот проигрывает их в реальном
    времени, пока параллельно идёт мост/добор через API.
    """
    ev = sorted(events, key=lambda e: (e["ts"], e["track_id"]))
    if not ev:
        return []
    step = max(1, len(ev) // n_frames)
    counter = Counter()
    info = {}
    frames = []
    for i, e in enumerate(ev):
        counter[e["track_id"]] += 1
        info.setdefault(e["track_id"], (e["track_name"], e["artist_name"]))
        if (i + 1) % step == 0 or i == len(ev) - 1:
            top = sorted(
                counter.items(), key=lambda kv: (-kv[1], info[kv[0]][0].lower()))[:10]
            frames.append({
                "t": e["ts"].strftime("%Y-%m-%d"),
                "top": [{"name": info[t][0], "artist": info[t][1], "plays": c}
                        for t, c in top],
            })
    return frames


def _read_bridge(track_ids):
    """map track_id → (album_id, release_date, total_tracks) из sqlite-индекса."""
    con = sqlite3.connect(TRACKS_IDX)
    con.execute("PRAGMA cache_size=-400000")
    links = []
    CH = 500
    for i in range(0, len(track_ids), CH):
        batch = track_ids[i:i + CH]
        q = "SELECT id, album_id FROM track_map WHERE id IN (%s)" % \
            ",".join("?" * len(batch))
        links.extend(con.execute(q, batch).fetchall())
    con.close()

    if not links:
        return pd.DataFrame(
            columns=["track_id", "sid", "release_date", "total_tracks"])

    sids = list({a for _, a in links})
    con = sqlite3.connect(ALBUMS_IDX)
    con.execute("PRAGMA cache_size=-400000")
    meta = {}
    for i in range(0, len(sids), CH):
        batch = sids[i:i + CH]
        q = "SELECT id, release_date, total_tracks FROM album_map " \
            "WHERE id IN (%s)" % ",".join("?" * len(batch))
        for sid, rd, tt in con.execute(q, batch):
            if rd is not None and tt is not None:
                meta[sid] = (rd, tt)
    con.close()

    rows = []
    for tid, sid in links:
        info = meta.get(sid)
        if info:
            rows.append((tid, sid, info[0], info[1]))
    return pd.DataFrame(
        rows, columns=["track_id", "sid", "release_date", "total_tracks"])


def normalize_release_date(date_str):
    """Приводит release_date Spotify к YYYY-MM-DD (year/month → первый день)."""
    if not date_str:
        return None
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str
    if len(date_str) == 7 and date_str[4] == '-':
        return f"{date_str}-01"
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    return None


def _read_cooldown():
    """Возвращает datetime, до которого API в кулдауне (или None)."""
    try:
        if COOLDOWN_FILE.exists():
            data = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
            return datetime.fromisoformat(data.get("until"))
    except Exception:
        pass
    return None


def _write_cooldown(until):
    """Записывает кулдаун до `until` (best-effort)."""
    try:
        COOLDOWN_FILE.write_text(
            json.dumps({"until": until.isoformat()}), encoding="utf-8")
    except Exception as exc:
        print(f"[cooldown] write failed: {exc!r}", file=sys.stderr)


def _is_429(e):
    """True, если исключение — rate limit Spotify (429), в т.ч. ошибка токена."""
    if getattr(e, "http_status", None) == 429:
        return True
    s = str(e).lower()
    return "429" in s or "rate limit" in s or "rate_limit" in s


def _prioritize_missing(events, missing):
    """Сортирует непокрытые треки по важности альбома: больше прослушиваний
    и больше уникальных треков — раньше."""
    missing_set = set(missing)
    album_tracks = {}
    album_plays = {}
    for e in events:
        tid = e["track_id"]
        if tid not in missing_set:
            continue
        key = (e["artist_name"], e["album_name"])
        album_tracks.setdefault(key, set()).add(tid)
        album_plays[key] = album_plays.get(key, 0) + 1
    keys = sorted(
        album_tracks,
        key=lambda k: (-album_plays[k], -len(album_tracks[k]),
                       k[1].lower(), k[0].lower()))
    ordered = []
    for k in keys:
        ordered.extend(sorted(album_tracks[k]))
    seen = set(ordered)
    ordered.extend(t for t in missing if t not in seen)
    return ordered


def _sync_note(summary, remaining):
    """Формирует человекочитаемый отчёт о неполноте добора (пусто, если всё покрыто)."""
    if not remaining:
        return ""
    why = []
    if summary.get("cooldown"):
        why.append("Spotify API cooldown")
    elif summary["limited"]:
        why.append("Spotify API limit")
    if summary["errors"]:
        why.append(f"{summary['errors']} API errors")
    if summary["skipped"]:
        why.append(f"over {MAX_API_CALLS}-request cap")
    suffix = f" ({', '.join(why)})" if why else ""
    return f"tracks without metadata: {remaining}{suffix}"


def _sync_missing(events, missing):
    """Добирает непокрытые треки через API, начиная с самых важных альбомов.

    При упоре в rate limit (429) обрывает добор и возвращает сводку.
    """
    from spotipy import Spotify
    from spotipy.oauth2 import SpotifyClientCredentials

    cooldown_until = _read_cooldown()
    if cooldown_until and datetime.now() < cooldown_until:
        return {"enriched": 0, "albums_added": 0, "errors": 0,
                "limited": True, "skipped": 0, "cooldown": True}

    sp = Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        ),
        retries=0,
        status_retries=0,
    )
    ordered = _prioritize_missing(events, missing)
    todo = ordered[:MAX_API_CALLS]
    skipped = max(0, len(ordered) - MAX_API_CALLS)

    existing = {}
    idx = sqlite3.connect(TRACKS_IDX)
    for i in range(0, len(todo), 500):
        batch = todo[i:i + 500]
        q = ("SELECT id, album_id FROM track_map WHERE id IN (%s)"
             % ",".join("?" * len(batch)))
        existing.update(idx.execute(q, batch).fetchall())
    idx.close()

    links, album_ids, errors = [], set(), 0
    limited = False
    for tid in todo:
        if tid in existing:
            album_ids.add(existing[tid])
            continue
        try:
            aid = sp.track(tid)["album"]["id"]
            links.append((tid, aid))
            album_ids.add(aid)
        except Exception as e:
            if _is_429(e):
                limited = True
                _write_cooldown(datetime.now() + timedelta(hours=COOLDOWN_HOURS))
                break
            errors += 1
        time.sleep(0.5)

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    al = ALBUMS_MIN.replace(chr(92), "/")
    need = []
    if album_ids:
        ids = list(album_ids)
        idx = sqlite3.connect(ALBUMS_IDX)
        have = set()
        for i in range(0, len(ids), 500):
            batch = ids[i:i + 500]
            q = ("SELECT id FROM album_map WHERE id IN (%s) "
                 "AND release_date IS NOT NULL AND total_tracks IS NOT NULL"
                 % ",".join("?" * len(batch)))
            have.update(r[0] for r in idx.execute(q, batch))
        idx.close()
        need = [aid for aid in ids if aid not in have]
    new_albums = []
    for aid in need:
        try:
            info = sp.album(aid)
            raw = info.get("release_date") or ""
            rd = None if raw.startswith("0000") else normalize_release_date(raw)
            new_albums.append((aid, rd, info.get("release_date_precision"),
                               info.get("total_tracks")))
        except Exception as e:
            if _is_429(e):
                limited = True
                _write_cooldown(datetime.now() + timedelta(hours=COOLDOWN_HOURS))
                break
            errors += 1
        time.sleep(0.5)

    tr = TRACKS_MIN.replace(chr(92), "/")
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    if links:
        con.execute("CREATE TABLE tl(id VARCHAR, album_id VARCHAR)")
        con.executemany("INSERT INTO tl VALUES (?,?)", links)
        con.execute(f"COPY tl TO '{tr}/delta-{stamp}.parquet' "
                    f"(FORMAT parquet, COMPRESSION zstd)")
        idx = sqlite3.connect(TRACKS_IDX)
        idx.executemany("INSERT OR REPLACE INTO track_map(id, album_id) "
                        "VALUES (?,?)", links)
        idx.commit()
        idx.close()
    if new_albums:
        con.execute("""CREATE TABLE na(
            id VARCHAR, release_date DATE, release_date_precision VARCHAR,
            total_tracks INTEGER)""")
        con.executemany("INSERT INTO na VALUES (?,?,?,?)", new_albums)
        con.execute(f"COPY na TO '{al}/delta-{stamp}.parquet' "
                    f"(FORMAT parquet, COMPRESSION zstd)")
        idx = sqlite3.connect(ALBUMS_IDX)
        idx.executemany("INSERT OR REPLACE INTO album_map"
                        "(id, release_date, release_date_precision, total_tracks) "
                        "VALUES (?,?,?,?)", new_albums)
        idx.commit()
        idx.close()
    con.close()

    return {
        "enriched": len(links),
        "albums_added": len(new_albums),
        "errors": errors,
        "limited": limited,
        "skipped": skipped,
    }


def analyze(events, use_api=True):
    """Строит данные дашборда из списка событий."""
    if not events:
        return {"error": "событий не найдено"}
    total = {e["ts"] for e in events}
    track_ids = list({e["track_id"] for e in events})
    if not track_ids:
        return {"error": "нет треков с spotify:track URI"}

    df = _read_bridge(track_ids)
    df["track_id"] = df["track_id"].astype(str)
    bridge = {"tid": {}, "sid": {}, "rd": {}, "tt": {}}
    for r in df.itertuples(index=False):
        bridge["tid"][r.track_id] = (r.sid, r.release_date, r.total_tracks)

    missing = [t for t in track_ids if t not in bridge["tid"]]
    note = None
    if missing and use_api:
        try:
            summary = _sync_missing(events, missing)
        except Exception as exc:
            note = f"tracks without metadata: {len(missing)} (enrichment failed)"
            try:
                print(f"[analyze] _sync_missing failed: {exc!r}", file=sys.stderr)
            except Exception:
                pass
        else:
            df = _read_bridge(track_ids)
            bridge = {"tid": {}, "sid": {}, "rd": {}, "tt": {}}
            for r in df.itertuples(index=False):
                bridge["tid"][r.track_id] = (r.sid, r.release_date, r.total_tracks)
            missing = [t for t in track_ids if t not in bridge["tid"]]
            note = _sync_note(summary, len(missing))
    elif missing:
        note = f"tracks without metadata: {len(missing)} (enrichment disabled)"

    groups = {}
    for e in events:
        key = (e["artist_name"], e["album_name"])
        groups.setdefault(key, []).append(e)

    albums = []
    for (artist_name, album_name), evs in groups.items():
        cand = {}
        for e in evs:
            info = bridge["tid"].get(e["track_id"])
            if not info:
                continue
            sid, rd, tt = info
            cnt = cand.setdefault(sid, {"rd": rd, "tt": tt, "tracks": set()})
            cnt["tracks"].add(e["track_id"])
        if not cand:
            continue
        best = min(cand, key=lambda s: (
            str(cand[s]["rd"]), -len(cand[s]["tracks"]), s))
        my_n = len(cand[best]["tracks"])
        tt = cand[best]["tt"]
        if tt is None or tt < 3 or my_n != tt:
            continue
        counts = Counter(e["track_id"] for e in evs
                         if bridge["tid"].get(e["track_id"], (None, None, None))[0] == best)
        tname = {e["track_id"]: e["track_name"] for e in evs}
        atracks = sorted(
            ({"name": tname[t], "plays": c} for t, c in counts.items()),
            key=lambda x: (-x["plays"], x["name"].lower()))
        full_plays = min(counts.values())
        albums.append({
            "album_name": album_name,
            "artist_name": artist_name,
            "release_year": int(str(cand[best]["rd"])[:4]),
            "plays": int(full_plays),
            "total_tracks": int(tt),
            "tracks": atracks,
        })

    years = sorted({a["release_year"] for a in albums})
    plays_by_year = Counter(a["release_year"] for a in albums)
    by_year = {y: sum(a["plays"] for a in albums if a["release_year"] == y)
               for y in years}
    artists = {}
    for a in albums:
        artists.setdefault(a["artist_name"], {
            "total": 0, "year": Counter(), "albums": {}})
    for a in albums:
        x = artists[a["artist_name"]]
        x["total"] += a["plays"]
        x["year"][a["release_year"]] += a["plays"]
        x["albums"].setdefault(a["release_year"], []).append(
            {"name": a["album_name"], "plays": a["plays"]})
    for x in artists.values():
        for lst in x["albums"].values():
            lst.sort(key=lambda z: -z["plays"])

    gen = {}
    for (artist_name, album_name), evs in groups.items():
        cand = {}
        for e in evs:
            info = bridge["tid"].get(e["track_id"])
            if not info:
                continue
            sid, rd, tt = info
            cnt = cand.setdefault(sid, {"rd": rd, "tt": tt, "tracks": set()})
            cnt["tracks"].add(e["track_id"])
        if cand:
            best = min(cand, key=lambda s: (
                str(cand[s]["rd"]), -len(cand[s]["tracks"]), s))
            listened = len(cand[best]["tracks"])
            total = int(cand[best]["tt"])
        else:
            listened = len({e["track_id"] for e in evs})
            total = None
        tcount = Counter(e["track_id"] for e in evs)
        tname = {e["track_id"]: e["track_name"] for e in evs}
        tracks = sorted(
            ({"name": tname[t], "plays": c} for t, c in tcount.items()),
            key=lambda x: (-x["plays"], x["name"].lower()))
        gen.setdefault(artist_name, []).append({
            "name": album_name, "plays": len(evs),
            "listened": listened, "total": total, "tracks": tracks})
    for lst in gen.values():
        lst.sort(key=lambda a: (-a["plays"], a["name"].lower()))

    tplays = Counter(e["track_id"] for e in events)
    tinfo = {}
    for e in events:
        tinfo.setdefault(e["track_id"], (e["track_name"], e["artist_name"]))
    top_tracks = sorted(
        ({"name": tinfo[t][0], "artist": tinfo[t][1], "plays": c}
         for t, c in tplays.items() if c >= MIN_TRACK_PLAYS),
        key=lambda x: (-x["plays"], x["name"].lower()))

    t0 = min(e["ts"] for e in events)
    t1 = max(e["ts"] for e in events)

    ats = {}
    for e in events:
        ats.setdefault(e["artist_name"], []).append(e["ts"])
    artist_dyn = []
    for a, tsl in ats.items():
        tsl.sort()
        artist_dyn.append({
            "name": a, "plays": len(tsl),
            "first": tsl[0].timestamp(),
            "nth": tsl[ARTIST_DYN_N - 1].timestamp()
                   if len(tsl) >= ARTIST_DYN_N else None,
        })
    artist_dyn.sort(key=lambda x: (-x["plays"], x["name"].lower()))

    tts = {}
    for e in events:
        tts.setdefault(e["track_id"], []).append(e["ts"])
    track_dyn = []
    for t, tsl in tts.items():
        tsl.sort()
        name, art = tinfo[t]
        track_dyn.append({
            "name": name, "artist": art, "plays": len(tsl),
            "first": tsl[0].timestamp(),
            "nth": tsl[TRACK_DYN_N - 1].timestamp()
                   if len(tsl) >= TRACK_DYN_N else None,
        })
    track_dyn.sort(key=lambda x: (-x["plays"], x["name"].lower()))

    days = Counter(e["ts"].date() for e in events)
    busiest_day, busiest_plays = max(days.items(), key=lambda kv: kv[1])
    ordered_days = sorted(days)
    streak_len, streak_start, streak_end = 1, ordered_days[0], ordered_days[0]
    run_len, run_start = 1, ordered_days[0]
    for prev, cur in zip(ordered_days, ordered_days[1:]):
        if (cur - prev).days == 1:
            run_len += 1
        else:
            run_len, run_start = 1, cur
        if run_len > streak_len:
            streak_len, streak_start, streak_end = run_len, run_start, cur
    active_days = len(days)
    gen_totals = {a: sum(x["plays"] for x in lst) for a, lst in gen.items()}
    top_artist = max(gen_totals, key=lambda a: (gen_totals[a], a.lower()))
    full_list = sorted(
        ({"album": a["album_name"], "artist": a["artist_name"],
          "plays": int(a["plays"]), "tracks": a["tracks"]} for a in albums),
        key=lambda x: (-x["plays"], x["album"].lower()))

    # Tracks by release year (same axis as albums: year of release)
    track_year = {}         # artist -> {year: plays}
    track_year_tracks = {}  # artist -> {year: {track_id: plays}}
    track_totals = Counter()
    for e in events:
        info = bridge["tid"].get(e["track_id"])
        if not info or not info[1]:
            continue
        y = str(info[1])[:4]
        if not y.isdigit():
            continue
        y = int(y)
        a = e["artist_name"]
        track_year.setdefault(a, Counter())[y] += 1
        track_year_tracks.setdefault(a, {}).setdefault(
            y, Counter())[e["track_id"]] += 1
        track_totals[a] += 1

    track_years = sorted({y for d in track_year.values() for y in d})
    track_plays_by_year = Counter(
        {y: sum(d[y] for d in track_year.values() if y in d)
         for y in track_years})
    keep = {a for a, c in track_totals.items() if c >= MIN_TRACK_PLAYS}
    track_artists = sorted(keep, key=lambda a: (-track_totals[a], a.lower()))
    track_artist_year = {a: {str(y): int(c) for y, c in track_year[a].items()}
                         for a in keep}
    track_artist_year_tracks = {}
    for a in keep:
        yd = track_year_tracks[a]
        tayt = {}
        for y, tc in yd.items():
            tayt[str(y)] = sorted(
                ({"name": tinfo[t][0], "plays": c} for t, c in tc.items()),
                key=lambda x: (-x["plays"], x["name"].lower()))
        track_artist_year_tracks[a] = tayt

    data = {
        "title": "Your Music Metrics",
        "note": note,
        "race": race_frames(events, 120),
        "years": years,
        "plays_by_year": {str(k): int(v) for k, v in by_year.items()},
        "artists": sorted(artists, key=lambda a: (-artists[a]["total"], a.lower())),
        "artist_year": {a: {str(k): int(v) for k, v in artists[a]["year"].items()}
                        for a in artists},
        "artist_year_albums": {a: {str(k): v for k, v in artists[a]["albums"].items()}
                               for a in artists},
        "artist_totals": {a: int(artists[a]["total"]) for a in artists},
        "general": {"artists": sorted(
            ({"name": a, "plays": sum(x["plays"] for x in lst), "albums": lst}
             for a, lst in gen.items()),
            key=lambda x: (-x["plays"], x["name"].lower()))},
        "tracks": top_tracks,
        "full_albums": full_list,
        "track_years": track_years,
        "track_plays_by_year": {str(k): int(v)
                                for k, v in track_plays_by_year.items()},
        "track_artists": track_artists,
        "track_artist_year": track_artist_year,
        "track_artist_totals": {a: int(track_totals[a]) for a in track_artists},
        "track_artist_year_tracks": track_artist_year_tracks,
        "time_range": {"min": t0.timestamp(), "max": t1.timestamp()},
        "artist_dynamics": artist_dyn,
        "track_dynamics": track_dyn,
        "artist_dyn_n": ARTIST_DYN_N,
        "track_dyn_n": TRACK_DYN_N,
        "stats": {
            "plays": len(events),
            "ms_total": sum(e["ms_played"] for e in events),
            "tracks": len(track_ids),
            "artists": len({e["artist_name"] for e in events}),
            "albums": len({(e["artist_name"], e["album_name"]) for e in events}),
            "full_albums": len(albums),
        },
        "top": {
            "artist": {"name": top_artist, "plays": int(gen_totals[top_artist])},
            "track": top_tracks[0] if top_tracks else None,
            "album": ({"name": full_list[0]["album"],
                       "artist": full_list[0]["artist"],
                       "plays": full_list[0]["plays"]}
                      if full_list else None),
        },
        "records": {
            "busiest": {"date": busiest_day.isoformat(), "plays": int(busiest_plays)},
            "streak": {"days": streak_len, "start": streak_start.isoformat(),
                       "end": streak_end.isoformat()},
            "active_days": active_days,
            "avg_per_day": round(len(events) / active_days, 1),
        },
    }
    return {"data": data, "events": len(events), "albums": len(albums),
            "artists": len(artists), "missing": len(missing), "note": note}


def render_dashboard(data):
    template = TEMPLATE.read_text(encoding="utf-8")
    return template.replace("__DATA__", json.dumps(data, ensure_ascii=False))