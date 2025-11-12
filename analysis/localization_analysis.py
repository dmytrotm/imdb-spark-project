import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql import Window

def localization_gaps(dataframes, save_path="."):
    """
    Виявлення ніш для локалізації:
    які неамериканські жанри мають високий середній рейтинг, але низький обсяг виробництва.
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    akas = dataframes["title.akas"]
    ratings = dataframes["title.ratings"]

    joined = basics.join(akas, basics["tconst"] == akas["titleId"]) \
                   .join(ratings, "tconst")

    films = joined.filter(F.col("titleType") == "movie")

    non_us = films.filter(F.col("region").isNotNull() & (F.col("region") != "US"))

    df = non_us.select("region", "genres", "averageRating")

    stats = df.groupBy("region", "genres").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("genres").alias("film_count")
    )

    us_stats = films.filter(F.col("region") == "US") \
                    .groupBy("genres") \
                    .agg(F.avg("averageRating").alias("us_avg_rating"),
                         F.count("genres").alias("us_film_count"))

    comparison = stats.join(us_stats, "genres", "left")

    comparison = comparison.withColumn("rating_gap", F.col("avg_rating") - F.col("us_avg_rating")) \
                           .withColumn("production_ratio", F.col("film_count") / F.col("us_film_count"))

    niches = comparison.filter(
        (F.col("rating_gap") > 0.3) & (F.col("production_ratio") < 0.3)
    ).orderBy(F.desc("rating_gap"))

    niches_pd = niches.toPandas()

    if not niches_pd.empty:
        plt.figure(figsize=(12, 6))
        sns.scatterplot(
            data=niches_pd,
            x="production_ratio",
            y="rating_gap",
            hue="region",
            size="film_count",
            sizes=(50, 300)
        )
        plt.title("Недооцінені ніші у неамериканських фільмах")
        plt.xlabel("Співвідношення кількості фільмів (до US)")
        plt.ylabel("Перевага в рейтингу над US")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "localization_gaps.png"))
        plt.show()

    return niches