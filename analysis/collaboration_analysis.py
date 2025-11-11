import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def writer_director_collaboration(dataframes, save_path="."):
    """
    Analyzes collaborations between writers and directors and the ratings of their joint films.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
    """
    os.makedirs(save_path, exist_ok=True)
    title_crew = dataframes["title.crew"]
    title_ratings = dataframes["title.ratings"]
    title_basics = dataframes["title.basics"]
    name_basics = dataframes["name.basics"]

    crew = title_crew.filter(F.col("directors").isNotNull() & F.col("writers").isNotNull())
    
    director_writer_pairs = crew.withColumn("director_nconst", F.explode(F.split(F.col("directors"), ","))) \
        .withColumn("writer_nconst", F.explode(F.split(F.col("writers"), ",")))

    director_names = name_basics.select(F.col("nconst").alias("director_nconst"), F.col("primaryName").alias("director_name"))
    writer_names = name_basics.select(F.col("nconst").alias("writer_nconst"), F.col("primaryName").alias("writer_name"))

    collaborations = director_writer_pairs.join(director_names, "director_nconst") \
        .join(writer_names, "writer_nconst")

    collaboration_counts = collaborations.groupBy("director_name", "writer_name") \
        .count().orderBy("count", ascending=False)

    # --- Visualization 1: Top 10 Collaborations ---
    top_pairs = collaboration_counts.limit(10)
    top_pairs_pd = top_pairs.toPandas()

    if not top_pairs_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=top_pairs_pd, x="count", y="director_name", hue="writer_name", dodge=False)
        plt.title("Top 10 Writer-Director Collaborations")
        plt.xlabel("Number of Collaborations")
        plt.ylabel("Director")
        plt.legend(title="Writer", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "writer_director_collaborations.png"))
        plt.show()

    # --- Visualization 2: Rating Trend by Year ---
    collaboration_ratings = collaborations.join(title_ratings, "tconst") \
        .join(title_basics, "tconst") \
        .groupBy("director_name", "writer_name", "startYear") \
        .agg(F.avg("averageRating").alias("avg_rating")) \
        .orderBy("startYear")
        
    top_pairs_ratings = collaboration_ratings.join(
        top_pairs.select("director_name", "writer_name"),
        ["director_name", "writer_name"]
    ).toPandas()

    if not top_pairs_ratings.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=top_pairs_ratings, x="startYear", y="avg_rating", hue="director_name", style="writer_name", marker="o")
        plt.title("Rating Trends of Top Writer-Director Collaborations by Year")
        plt.xlabel("Year")
        plt.ylabel("Average Rating")
        plt.legend(title="Collaboration", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "writer_director_ratings.png"))
        plt.show()

    # --- Visualization 3: Sequential Rating Trend ---
    pair_film_ratings = collaborations.join(title_ratings, "tconst").join(title_basics, "tconst")

    window_spec = Window.partitionBy("director_name", "writer_name").orderBy("startYear")
    ranked_films = pair_film_ratings.withColumn("collaboration_rank", F.row_number().over(window_spec))

    ranked_films_top_pairs = ranked_films.join(
        top_pairs.select("director_name", "writer_name"),
        ["director_name", "writer_name"]
    )

    sequential_rating_trend = ranked_films_top_pairs.groupBy("collaboration_rank") \
        .agg(F.avg("averageRating").alias("avg_rating_by_rank")) \
        .orderBy("collaboration_rank")
        
    sequential_rating_trend_pd = sequential_rating_trend.toPandas()

    if not sequential_rating_trend_pd.empty:
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=sequential_rating_trend_pd, x="collaboration_rank", y="avg_rating_by_rank", marker="o")
        plt.title("Average Rating Trend by Collaboration Sequence (Top Pairs)")
        plt.xlabel("Collaboration Number (1st, 2nd, etc.)")
        plt.ylabel("Average Rating")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "writer_director_sequential_ratings.png"))
        plt.show()


def actor_director_collaboration(dataframes, save_path="."):
    """
    Analyzes collaborations between actors and directors and the ratings of their joint films.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
    """
    os.makedirs(save_path, exist_ok=True)
    title_principals = dataframes["title.principals"]
    title_crew = dataframes["title.crew"]
    title_ratings = dataframes["title.ratings"]
    title_basics = dataframes["title.basics"]
    name_basics = dataframes["name.basics"]

    actors = title_principals.filter(F.col("category").isin(["actor", "actress"])) \
        .select(F.col("tconst"), F.col("nconst").alias("actor_nconst"))

    directors = title_crew.filter(F.col("directors").isNotNull()) \
        .withColumn("director_nconst", F.explode(F.split(F.col("directors"), ","))) \
        .select("tconst", "director_nconst")

    collaborations = actors.join(directors, "tconst")

    actor_names = name_basics.select(F.col("nconst").alias("actor_nconst"), F.col("primaryName").alias("actor_name"))
    director_names = name_basics.select(F.col("nconst").alias("director_nconst"), F.col("primaryName").alias("director_name"))

    collaborations_with_names = collaborations.join(actor_names, "actor_nconst") \
        .join(director_names, "director_nconst")

    collaboration_counts = collaborations_with_names.groupBy("actor_name", "director_name") \
        .count().orderBy("count", ascending=False)

    collaboration_ratings = collaborations_with_names.join(title_ratings, "tconst") \
        .join(title_basics, "tconst") \
        .groupBy("actor_name", "director_name", "startYear") \
        .agg(F.avg("averageRating").alias("avg_rating")) \
        .orderBy("startYear")

    top_pairs = collaboration_counts.limit(10)
    top_pairs_pd = top_pairs.toPandas()

    if not top_pairs_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=top_pairs_pd, x="count", y="director_name", hue="actor_name", dodge=False)
        plt.title("Top 10 Actor-Director Collaborations")
        plt.xlabel("Number of Collaborations")
        plt.ylabel("Director")
        plt.legend(title="Actor", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "actor_director_collaborations.png"))
        plt.show()

    top_pairs_ratings = collaboration_ratings.join(
        top_pairs.select("actor_name", "director_name"),
        ["actor_name", "director_name"]
    ).toPandas()

    if not top_pairs_ratings.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=top_pairs_ratings, x="startYear", y="avg_rating", hue="director_name", style="actor_name", marker="o")
        plt.title("Rating Trends of Top Actor-Director Collaborations")
        plt.xlabel("Year")
        plt.ylabel("Average Rating")
        plt.legend(title="Collaboration", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "actor_director_ratings.png"))
        plt.show()
