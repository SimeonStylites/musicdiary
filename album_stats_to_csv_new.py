import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def main():
    conn = get_connection()
    
    query = """
        WITH track_plays AS (
            SELECT 
                a.album_name,
                ar.artist_name,
                a.spotify_release_date,
                a.spotify_total_tracks,
                le.track_id,
                COUNT(*) as track_play_count
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
                album_name,
                artist_name,
                spotify_release_date,
                spotify_total_tracks,
                COUNT(DISTINCT track_id) as listened_tracks,
                MIN(track_play_count) as full_album_plays
            FROM track_plays
            GROUP BY album_name, artist_name, spotify_release_date, spotify_total_tracks
            HAVING COUNT(DISTINCT track_id) = spotify_total_tracks
        )
        SELECT 
            album_name,
            artist_name,
            full_album_plays,
            EXTRACT(YEAR FROM spotify_release_date) as release_year,
            spotify_total_tracks as total_tracks
        FROM album_full_plays
        WHERE spotify_total_tracks >= 3
        ORDER BY release_year, artist_name
    """
    
    df = pd.read_sql(query, conn)
    
    #Saving csv
    output_file = "album_full_plays.csv"
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Export {len(df)} albums in {output_file}")
    print(f"Fields: {', '.join(df.columns)}")
    print(f"Years: since {df['release_year'].min()} till {df['release_year'].max()}")
    print(f"Total listened times: {df['full_album_plays'].sum()}")
    
    conn.close()

if __name__ == "__main__":
    main()