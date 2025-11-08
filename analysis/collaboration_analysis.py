import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F


def writer_director_collaboration(dataframes, save_path="."):
    """
    Analyzes collaborations between writers and directors and the ratings of their joint films.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
    """
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

    collaboration_ratings = collaborations.join(title_ratings, "tconst") \
        .join(title_basics, "tconst") \
        .groupBy("director_name", "writer_name", "startYear") \
        .agg(F.avg("averageRating").alias("avg_rating")) \
        .orderBy("startYear")

    top_pairs = collaboration_counts.limit(10)
    top_pairs_pd = top_pairs.toPandas()

    if not top_pairs_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=top_pairs_pd, x="count", y="director_name", hue="writer_name", dodge=False)
        plt.title("Top 10 Writer-Director Collaborations")
        plt.xlabel("Number of Collaborations")
        plt.ylabel("Director")
        plt.legend(title="Writer")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "writer_director_collaborations.png"))
        plt.show()

    top_pairs_ratings = collaboration_ratings.join(
        top_pairs.select("director_name", "writer_name"),
        ["director_name", "writer_name"]
    ).toPandas()

    if not top_pairs_ratings.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=top_pairs_ratings, x="startYear", y="avg_rating", hue="director_name", style="writer_name", marker="o")
        plt.title("Rating Trends of Top Writer-Director Collaborations")
        plt.xlabel("Year")
        plt.ylabel("Average Rating")
        plt.legend(title="Collaboration")
        plt.grid(True)
        plt.savefig(os.path.join(save_path, "writer_director_ratings.png"))
        plt.show()
