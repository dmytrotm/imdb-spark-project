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
    top_pairs_pd["collaboration"] = top_pairs_pd["director_name"] + " - " + top_pairs_pd["writer_name"]

    if not top_pairs_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=top_pairs_pd, x="count", y="collaboration")
        plt.title("Top 10 Writer-Director Collaborations")
        plt.xlabel("Number of Collaborations")
        plt.ylabel("Collaboration")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "writer_director_collaborations.png"))
        plt.show()

    # --- Visualization 2: Average rating of collaborations ---
    avg_rating_collaborations = collaborations.join(title_ratings, "tconst") \
        .groupBy("director_name", "writer_name") \
        .agg(F.avg("averageRating").alias("avg_rating"))

    top_pairs_with_avg_rating = top_pairs.join(avg_rating_collaborations, ["director_name", "writer_name"])

    top_pairs_with_avg_rating_pd = top_pairs_with_avg_rating.toPandas()
    top_pairs_with_avg_rating_pd["collaboration"] = top_pairs_with_avg_rating_pd["director_name"] + " - " + top_pairs_with_avg_rating_pd["writer_name"]

    if not top_pairs_with_avg_rating_pd.empty:
        plt.figure(figsize=(12, 8))
        ax = sns.barplot(data=top_pairs_with_avg_rating_pd, x="avg_rating", y="collaboration", orient='h')
        plt.title("Average Rating of Top 10 Writer-Director Collaborations")
        plt.xlabel("Average Rating")
        plt.ylabel("Writer-Director Collaboration")
        plt.tight_layout()
        # Add ratings on bars
        for i in ax.containers:
            ax.bar_label(i,)
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

    # --- Visualization 1: Top 10 Collaborations ---
    top_pairs = collaboration_counts.limit(10)
    top_pairs_pd = top_pairs.toPandas()
    top_pairs_pd["collaboration"] = top_pairs_pd["director_name"] + " - " + top_pairs_pd["actor_name"]

    if not top_pairs_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=top_pairs_pd, x="count", y="collaboration")
        plt.title("Top 10 Actor-Director Collaborations")
        plt.xlabel("Number of Collaborations")
        plt.ylabel("Collaboration")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "actor_director_collaborations.png"))
        plt.show()

    # --- Visualization 2: Average rating of collaborations ---
    avg_rating_collaborations = collaborations_with_names.join(title_ratings, "tconst") \
        .groupBy("director_name", "actor_name") \
        .agg(F.avg("averageRating").alias("avg_rating"))

    top_pairs_with_avg_rating = top_pairs.join(avg_rating_collaborations, ["director_name", "actor_name"])

    top_pairs_with_avg_rating_pd = top_pairs_with_avg_rating.toPandas()
    top_pairs_with_avg_rating_pd["collaboration"] = top_pairs_with_avg_rating_pd["director_name"] + " - " + top_pairs_with_avg_rating_pd["actor_name"]

    if not top_pairs_with_avg_rating_pd.empty:
        plt.figure(figsize=(12, 8))
        ax = sns.barplot(data=top_pairs_with_avg_rating_pd, x="avg_rating", y="collaboration", orient='h')
        plt.title("Average Rating of Top 10 Actor-Director Collaborations")
        plt.xlabel("Average Rating")
        plt.ylabel("Actor-Director Collaboration")
        plt.tight_layout()
        # Add ratings on bars
        for i in ax.containers:
            ax.bar_label(i,)
        plt.savefig(os.path.join(save_path, "actor_director_ratings.png"))
        plt.show()
