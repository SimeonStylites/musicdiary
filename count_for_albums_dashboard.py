import pandas as pd

df = pd.read_csv('album_full_plays_by_release_year.csv')

total_albums = df['album_name'].nunique()
since_2000 = df[df['release_year'] >= 2000]['album_name'].nunique()
since_2020 = df[df['release_year'] >= 2020]['album_name'].nunique()
total_artists = df['artist_name'].nunique()
since_2020_plays = df[df['release_year'] >= 2020]['full_album_plays'].sum()

print(f"Total albums: {total_albums}")
print(f"Since 2000: {since_2000}")
print(f"Since 2020: {since_2020}")
print(f"Total full album plays since 2020: {since_2020_plays}")
print(f"Unique artists: {total_artists}")