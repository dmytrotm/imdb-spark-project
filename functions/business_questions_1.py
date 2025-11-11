from pyspark.sql.functions import explode, split, col, avg, count, desc, row_number, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql import functions as F


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
    tv_with_ratings = tv_with_seasons.join(ratings, "tconst")

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


def actors_demography_stats(datasets):
    """
    Соціальна демографія акторів: вік (birthYear), активність (уникальна кількість фільмів), середній рейтинг
    """
    # Таблиці
    name_basics = datasets["name.basics"]
    principals = datasets["title.principals"]
    ratings = datasets["title.ratings"]

    # Беремо тільки акторів та актрис
    actors = principals.filter(col("category").isin(["actor", "actress"]))

    # Обчислюємо унікальні пари актор-фільм (щоб не рахувати дублікати)
    distinct_works = actors.select("nconst", "tconst").distinct()

    # Підрахунок унікальної кількості фільмів/серіалів на актора
    film_counts = distinct_works.groupBy("nconst").agg(count("tconst").alias("num_titles"))

    # Об’єднуємо з інформацією про акторів (ім'я та рік народження)
    actors_info = film_counts.join(
        name_basics.select("nconst", "primaryName", "birthYear"),
        "nconst"
    )

    # Обчислюємо середній рейтинг для кожного актора
    actors_films = distinct_works.join(ratings, "tconst")
    avg_ratings = actors_films.groupBy("nconst").agg(avg("averageRating").alias("avg_rating"))

    # Об’єднуємо всі дані
    actors_info = actors_info.join(avg_ratings, "nconst")

    # Відкидаємо дублі, сортуємо за активністю та обираємо топ 30
    result = actors_info.dropDuplicates().orderBy(desc("num_titles")).limit(30)

    return result


def genre_seasons_influence(datasets):
    """
    Як жанрове різноманіття впливає на кількість сезонів серіалів
    """
    basics = datasets["title.basics"]
    episodes = datasets["title.episode"]

    # Беремо тільки серіали
    series = basics.filter(col("titleType") == "tvSeries").select("tconst", "genres")
    # Считаємо max seasonNumber для кожного parentTconst
    seasons = episodes.groupBy("parentTconst").agg(spark_max("seasonNumber").alias("num_seasons"))
    # Об'єднуємо жанри серіалів з їх кількістю сезонів
    series_with_genres = series.join(seasons, series.tconst == seasons.parentTconst)
    # Вибухаємо жанри, так як це строка з ',' (в IMDB — genres: str, наприклад 'Drama,Action')
    exploded = series_with_genres.withColumn("genre", explode(split("genres", ","))).select("genre", "num_seasons")
    # Групуємо за жанром — середня, максимальна, мінімальна кількість сезонів
    stats = exploded.groupBy("genre").agg(
        avg("num_seasons").alias("avg_seasons"),
        spark_max("num_seasons").alias("max_seasons"),
        F.min("num_seasons").alias("min_seasons"),
        count("genre").alias("num_series")
    ).orderBy(desc("avg_seasons"))

    return stats