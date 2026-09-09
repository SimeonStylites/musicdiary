import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm
import os

DATA_FILE = "album_full_plays.csv"
OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def setup_ru_font():
    for font_name in ["DejaVu Sans", "Arial", "Segoe UI"]:
        try:
            plt.rcParams["font.family"] = font_name
            break
        except Exception:
            continue
    plt.rcParams["axes.unicode_minus"] = False


def load_data():
    df = pd.read_csv(DATA_FILE, encoding="utf-8")
    df["release_year"] = df["release_year"].astype(int)
    df["full_album_plays"] = df["full_album_plays"].astype(int)
    return df


def save(fig, name):
    path = os.path.join(OUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_by_year(df):
    by_year = df.groupby("release_year").agg(
        albums=("album_name", "nunique"),
        plays=("full_album_plays", "sum"),
    ).reset_index()

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    years = sorted(by_year["release_year"].unique())
    ticks = years[::5] if len(years) > 20 else years

    sns.barplot(data=by_year, x="release_year", y="albums", hue="release_year",
                ax=axes[0], palette="rocket", legend=False)
    axes[0].set_title("Полностью прослушанные альбомы по году релиза")
    axes[0].set_xlabel("Год релиза")
    axes[0].set_ylabel("Кол-во альбомов")
    axes[0].set_xticks([by_year["release_year"].tolist().index(t) for t in ticks])
    axes[0].set_xticklabels(ticks)
    axes[0].tick_params(axis="x", rotation=45)

    sns.barplot(data=by_year, x="release_year", y="plays", hue="release_year",
                ax=axes[1], palette="viridis", legend=False)
    axes[1].set_title("Суммарные полные прослушивания по году релиза")
    axes[1].set_xlabel("Год релиза")
    axes[1].set_ylabel("Всего прослушиваний")
    axes[1].set_xticks([by_year["release_year"].tolist().index(t) for t in ticks])
    axes[1].set_xticklabels(ticks)
    axes[1].tick_params(axis="x", rotation=45)
    fig.tight_layout()
    save(fig, "album_by_year.png")


def plot_top_albums(df, title, fname, n=15):
    agg = df.groupby(["album_name", "artist_name"], as_index=False)["full_album_plays"].sum()
    agg = agg.sort_values("full_album_plays", ascending=False).head(n)
    agg["label"] = agg["artist_name"] + " — " + agg["album_name"]

    fig, ax = plt.subplots(figsize=(12, max(6, n * 0.5)))
    sns.barplot(data=agg, y="label", x="full_album_plays", hue="label",
                ax=ax, palette="rocket", legend=False)
    ax.set_title(title)
    ax.set_xlabel("Полные прослушивания")
    ax.set_ylabel("")
    fig.tight_layout()
    save(fig, fname)


def plot_top_artists(df, n=15):
    agg = df.groupby("artist_name", as_index=False)["album_name"].nunique()
    agg = agg.rename(columns={"album_name": "albums"})
    agg = agg.sort_values("albums", ascending=False).head(n)

    fig, ax = plt.subplots(figsize=(12, max(6, n * 0.5)))
    sns.barplot(data=agg, y="artist_name", x="albums", hue="artist_name",
                ax=ax, palette="viridis", legend=False)
    ax.set_title("Исполнители с наибольшим числом полностью прослушанных альбомов")
    ax.set_xlabel("Число альбомов")
    ax.set_ylabel("")
    fig.tight_layout()
    save(fig, "top_artists.png")


def main():
    setup_ru_font()
    df = load_data()

    total_albums = df["album_name"].nunique()
    total_artists = df["artist_name"].nunique()
    total_plays = df["full_album_plays"].sum()
    since_2020_albums = df[df["release_year"] >= 2020]["album_name"].nunique()
    since_2020_plays = df[df["release_year"] >= 2020]["full_album_plays"].sum()

    print(f"Total albums: {total_albums}")
    print(f"Unique artists: {total_artists}")
    print(f"Total full album plays: {total_plays}")
    print(f"Albums since 2020: {since_2020_albums}")
    print(f"Full album plays since 2020: {since_2020_plays}")

    plot_by_year(df)
    plot_top_albums(df, "Топ альбомов по полным прослушиваниям", "top_albums.png")
    plot_top_albums(df[df["release_year"] >= 2020],
                    "Топ альбомов с 2020 г. по полным прослушиваниям",
                    "top_albums_since_2020.png")
    plot_top_artists(df)


if __name__ == "__main__":
    main()
