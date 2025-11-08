from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, avg, count, desc, row_number, max as spark_max
from pyspark.sql.window import Window

def writers_directors_collaboration_trend(datasets):
    title_crew = datasets["title.crew"]
    ratings = datasets["title.ratings"]
    basics = datasets["title.basics"]

    writers = title_crew.select(
        col("tconst"),
        explode(split(col("writers"), ",")).alias("writer")
    )
    directors = title_crew.select(
        col("tconst"),
        explode(split(col("directors"), ",")).alias("director")
    )

    crew_pairs = writers.join(directors, "tconst") \
                        .join(basics.select("tconst", "startYear"), "tconst") \
                        .join(ratings, "tconst")

    collab_stats = crew_pairs.groupBy("writer", "director").agg(
        count("tconst").alias("num_films"),
        avg("averageRating").alias("avg_rating")
    ).filter(col("num_films") > 1)

    window_spec = Window.partitionBy("writer", "director").orderBy("startYear")
    trend_df = crew_pairs.withColumn("rank", row_number().over(window_spec))
    trend_avg = trend_df.groupBy("writer", "director", "rank").agg(avg("averageRating").alias("rating_trend"))

    return collab_stats.orderBy(desc("num_films")).limit(20)

def top_directors_by_high_rating_and_votes(datasets):
    title_crew = datasets["title.crew"]
    ratings = datasets["title.ratings"]
    basics = datasets["title.basics"]

    directors = title_crew.select(
        col("tconst"),
        explode(split(col("directors"), ",")).alias("director")
    )

    directors_rated = directors.join(ratings, "tconst") \
                               .join(basics.select("tconst", "startYear"), "tconst")

    directors_rated = directors_rated.filter(col("averageRating") > 8)

    result = directors_rated.groupBy("director", "startYear").agg(
        count("tconst").alias("num_films"),
        avg("numVotes").alias("avg_votes")
    ).orderBy("director", "startYear")

    return result

def correlation_seasons_rating(datasets):
    episodes = datasets["title.episode"]
    ratings = datasets["title.ratings"]
    basics = datasets["title.basics"].filter(col("titleType") == "tvSeries")

    seasons_count = episodes.groupBy("parentTconst").agg(spark_max("seasonNumber").alias("num_seasons"))
    tv_with_seasons = basics.join(seasons_count, basics.tconst == seasons_count.parentTconst).select(basics.tconst, "num_seasons")
    tv_with_ratings = tv_with_seasons.join(ratings, tv_with_seasons.tconst == ratings.tconst)

    corr_data = tv_with_ratings.groupBy("num_seasons").agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("count")
    ).orderBy("num_seasons")

    return corr_data

def top_episodes_by_votes_and_rating(datasets):
    episodes = datasets["title.episode"]
    ratings = datasets["title.ratings"]
    basics = datasets["title.basics"]

    ep_with_ratings = episodes.join(ratings, "tconst") \
                              .join(basics.select("tconst", "primaryTitle"), "tconst")

    top_episodes = ep_with_ratings.orderBy(desc("numVotes")).select(
        "tconst", "primaryTitle", "numVotes", "averageRating"
    ).limit(20)

    return top_episodes