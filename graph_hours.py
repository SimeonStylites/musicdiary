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

#Activity by hour
merged['hour'] = pd.to_datetime(merged['played_at']).dt.hour
hourly = merged.groupby('hour').size().reset_index(name='count')

#Graphic (hours)
sns.set_style('darkgrid')
plt.figure(figsize=(12, 5))
sns.barplot(data=hourly, x='hour', y='count', hue='hour',
            palette='viridis', legend=False)
plt.title('Activity by hour')
plt.xlabel('Hour')
plt.ylabel('Listened')
plt.tight_layout()
#plt.savefig('hourly_activity.png')
plt.show()