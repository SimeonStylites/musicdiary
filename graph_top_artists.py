import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import os

load_dotenv()

#Create engine with SQLAlchemy
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

#Reading tables with engine engine
df_events = pd.read_sql("SELECT * FROM listening_events", engine)
df_albums = pd.read_sql("SELECT * FROM albums", engine)
df_artists = pd.read_sql("SELECT * FROM artists", engine)

#Merging
merged = df_events.merge(df_albums, on='album_id').merge(df_artists, on='artist_id')

#Top artists
top_artists = merged.groupby('artist_name').size().reset_index(name='plays')
top_artists = top_artists.sort_values('plays', ascending=False).head(10)

#Diagram
plt.figure(figsize=(10, 6))
sns.barplot(data=top_artists, y='artist_name', x='plays', hue='artist_name',
            palette='rocket', legend=False)
plt.title('Top-10 artists')
plt.xlabel('Listened')
plt.ylabel('Artist')
plt.tight_layout()
#plt.savefig('top_artists.png')
plt.show()