# export_for_album_dashboard.py
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

query = """
    -- 1.count all listenings of all tracks 
    WITH track_plays AS (
        SELECT 
            a.album_id,
            a.album_name,
            a.release_date,
            a.total_tracks,
            ar.artist_name,
            le.track_id,
            COUNT(*) as track_play_count
        FROM listening_events le
        JOIN albums a ON le.album_id = a.album_id
        JOIN artists ar ON a.artist_id = ar.artist_id
        WHERE a.release_date IS NOT NULL AND a.total_tracks IS NOT NULL
        GROUP BY a.album_id, a.album_name, a.release_date, a.total_tracks, ar.artist_name, le.track_id
    ),
    -- 2.how many unique tracks listened in album
    album_coverage AS (
        SELECT 
            album_id,
            album_name,
            artist_name,
            release_date,
            total_tracks,
            COUNT(DISTINCT track_id) as listened_tracks
        FROM track_plays
        GROUP BY album_id, album_name, artist_name, release_date, total_tracks
    ),
    -- 3.only entirely listened albums
    fully_listened_albums AS (
        SELECT 
            tc.album_id,
            tc.album_name,
            tc.artist_name,
            tc.release_date,
            tc.total_tracks,
            tp.track_id,
            tp.track_play_count
        FROM album_coverage tc
        JOIN track_plays tp ON tc.album_id = tp.album_id
        WHERE tc.listened_tracks = tc.total_tracks  -- только полностью прослушанные
    ),
    -- 4.for each min(track_play_count) is full album plays
    album_full_plays AS (
        SELECT 
            album_id,
            album_name,
            artist_name,
            release_date,
            total_tracks,
            MIN(track_play_count) as full_album_plays
        FROM fully_listened_albums
        GROUP BY album_id, album_name, artist_name, release_date, total_tracks
    )
    -- 5.only albums >2 tracks by year 
    SELECT 
        album_name,
        artist_name,
        EXTRACT(YEAR FROM release_date) as release_year,
        full_album_plays,
        total_tracks
    FROM album_full_plays
    WHERE total_tracks >= 3
    ORDER BY release_year, artist_name
"""

df = pd.read_sql(query, engine)
df.to_csv('album_full_plays_by_release_year.csv', index=False)
print(f"Export {len(df)} albums")
print(df.head(15))
