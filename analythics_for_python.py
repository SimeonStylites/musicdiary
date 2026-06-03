import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))

#Expport data to DataFrame
df_events = pd.read_sql("SELECT * FROM listening_events", conn)
df_albums = pd.read_sql("SELECT * FROM albums", conn)
df_artists = pd.read_sql("SELECT * FROM artists", conn)

#Merging
merged = df_events.merge(df_albums, on='album_id').merge(df_artists, on='artist_id')

#Top10 artists
top_artists = merged.groupby('artist_name').size().sort_values(ascending=False).head(10)

#Activity by hour
hourly = merged.groupby(merged['played_at'].dt.hour).size()

#Graphic (hours)
plt.figure(figsize=(12, 5))
plt.bar(hourly.index, hourly.values, color='steelblue')
plt.title('Activity by hour')
plt.xlabel('Hour')
plt.ylabel('Listened')
plt.xticks(range(0, 24))
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
#plt.savefig('hourly_activity.png')
plt.show()