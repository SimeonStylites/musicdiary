import pandas as pd
import json
import os

DATA_FILE = "album_full_plays.csv"
HTML_FILE = "album_dashboard.html"
PAGES_FILE = os.path.join("github-pages", "index.html")
TEMPLATE_FILE = "album_dashboard_template.html"

df = pd.read_csv(DATA_FILE, encoding="utf-8")
df["release_year"] = df["release_year"].astype(int)
df["full_album_plays"] = df["full_album_plays"].astype(int)

years = sorted(int(y) for y in df["release_year"].unique())

plays_by_year = df.groupby("release_year")["full_album_plays"].sum().to_dict()

artist_totals = df.groupby("artist_name")["full_album_plays"].sum().to_dict()
artists = sorted(artist_totals, key=lambda a: (-artist_totals[a], a.lower()))

artist_year = {}
for a in artists:
    sub = df[df["artist_name"] == a].groupby("release_year")["full_album_plays"].sum()
    artist_year[a] = {int(y): int(v) for y, v in sub.items()}

data = {
    "years": years,
    "plays_by_year": {int(k): int(v) for k, v in plays_by_year.items()},
    "artists": artists,
    "artist_year": artist_year,
    "artist_totals": {a: int(artist_totals[a]) for a in artists},
}

with open(TEMPLATE_FILE, "r", encoding="utf-8") as tf:
    template = tf.read()

html = template.replace("__DATA__", json.dumps(data, ensure_ascii=False))

with open(HTML_FILE, "w", encoding="utf-8") as f:
    f.write(html)

os.makedirs(os.path.dirname(PAGES_FILE), exist_ok=True)
with open(PAGES_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Saved: {HTML_FILE}")
print(f"Saved: {PAGES_FILE}")
print(f"Years: {len(years)} ({years[0]} to {years[-1]})")
print(f"Artists: {len(artists)}")
