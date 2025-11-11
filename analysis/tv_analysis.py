import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def correlation_seasons_rating(dataframes, save_path="."):
    os.makedirs(save_path, exist_ok=True)

    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"].filter(F.col("titleType") == "tvSeries")

    seasons_count = episodes.groupBy("parentTconst").agg(F.max("seasonNumber").alias("num_seasons"))
    tv_with_seasons = basics.join(seasons_count, basics.tconst == seasons_count.parentTconst).select(basics.tconst, "num_seasons")
    tv_with_ratings = tv_with_seasons.join(ratings, "tconst")

    corr_data = tv_with_ratings.groupBy("num_seasons").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("count")
    ).orderBy("num_seasons")

    corr_data_pd = corr_data.toPandas()

    if not corr_data_pd.empty:
        plt.figure(figsize=(12, 7))
        sns.scatterplot(data=corr_data_pd, x="num_seasons", y="avg_rating", size="count", hue="count", sizes=(50, 500), alpha=0.7)
        plt.title("Correlation Between Number of Seasons and Average Rating")
        plt.xlabel("Number of Seasons")
        plt.ylabel("Average Rating")
        plt.legend(title="Number of Series")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "seasons_rating_correlation.png"))
        plt.show()

    return corr_data


def top_episodes_by_votes_and_rating(dataframes, save_path="."):
    os.makedirs(save_path, exist_ok=True)
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"]

    ep_with_ratings = episodes.join(ratings, "tconst") \
                              .join(basics.select("tconst", "primaryTitle"), "tconst")

    top_episodes = ep_with_ratings.orderBy(F.desc("numVotes")).select(
        "tconst", "primaryTitle", "numVotes", "averageRating"
    ).limit(20)

    top_episodes_pd = top_episodes.toPandas()

    if not top_episodes_pd.empty:
        plt.figure(figsize=(12, 10))
        sns.barplot(data=top_episodes_pd, x="numVotes", y="primaryTitle", orient='h')
        plt.title("Top 20 TV Episodes by Number of Votes")
        plt.xlabel("Number of Votes")
        plt.ylabel("Episode Title")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "top_episodes_by_votes.png"))
        plt.show()

    return top_episodes


def genre_seasons_influence(dataframes, save_path="."):
    """
    Як жанрове різноманіття впливає на кількість сезонів серіалів
    """
    os.makedirs(save_path, exist_ok=True)
    
    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]

    series = basics.filter(F.col("titleType") == "tvSeries").select("tconst", "genres")
    seasons = episodes.groupBy("parentTconst").agg(F.max("seasonNumber").alias("num_seasons"))
    series_with_genres = series.join(seasons, series.tconst == seasons.parentTconst)
    exploded = series_with_genres.withColumn("genre", F.explode(F.split("genres", ","))).select("genre", "num_seasons")
    
    stats = exploded.groupBy("genre").agg(
        F.avg("num_seasons").alias("avg_seasons"),
        F.max("num_seasons").alias("max_seasons"),
        F.min("num_seasons").alias("min_seasons"),
        F.count("genre").alias("num_series")
    ).orderBy(F.desc("avg_seasons"))

    stats_pd = stats.filter(F.col("num_series") > 100).toPandas() # Filter for genres with a decent number of series

    if not stats_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=stats_pd, x="avg_seasons", y="genre", orient='h')
        plt.title("Influence of Genre on Average Number of Seasons")
        plt.xlabel("Average Number of Seasons")
        plt.ylabel("Genre")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "genre_seasons_influence.png"))
        plt.show()

    return stats
