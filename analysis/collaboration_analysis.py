import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from analysis.general import filter_by_region


def writer_director_collaboration(dataframes, save_path=".", top_n_regions=5):
    """Analyzes writer-director collaborations by region."""
    os.makedirs(save_path, exist_ok=True)
    title_crew = dataframes["title.crew"]
    title_ratings = dataframes["title.ratings"]
    title_basics = dataframes["title.basics"]
    name_basics = dataframes["name.basics"]
    title_akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(title_akas, top_n_regions)
    print(f"Analyzing writer-director collaborations for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        title_akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter for movies only
    title_basics_movies = title_basics.filter(F.col("titleType") == "movie")

    crew = title_crew.filter(F.col("directors").isNotNull() & F.col("writers").isNotNull())
    
    director_writer_pairs = crew.withColumn("director_nconst", F.explode(F.split(F.col("directors"), ","))) \
        .withColumn("writer_nconst", F.explode(F.split(F.col("writers"), ",")))

    director_names = name_basics.select(F.col("nconst").alias("director_nconst"), F.col("primaryName").alias("director_name"))
    writer_names = name_basics.select(F.col("nconst").alias("writer_nconst"), F.col("primaryName").alias("writer_name"))

    collaborations = (
        director_writer_pairs.join(director_names, "director_nconst")
        .join(writer_names, "writer_nconst")
        .join(title_basics_movies.select("tconst"), "tconst")
        .join(regional_akas, "tconst")
    )

    collaboration_counts = (
        collaborations.groupBy("region", "director_name", "writer_name")
        .agg(
            F.count("tconst").alias("count"),
            F.avg("director_name").alias("_dummy")  # Just to keep structure
        )
        .drop("_dummy")
    )

    # Get top collaborations per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("count"))
    top_collabs = (
        collaboration_counts.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= 10)
        .orderBy("region", "rank")
    )

    result_pd = top_collabs.toPandas()

    if not result_pd.empty:
        result_pd["collaboration"] = result_pd["director_name"] + " - " + result_pd["writer_name"]

        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(10)

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="count",
                    y="collaboration",
                    hue="collaboration",
                    palette="mako",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Top Writer-Director Pairs in {region}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Number of Collaborations", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Writer-Director Collaborations in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "writer_director_collaborations_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_collabs


def actor_director_collaboration(dataframes, save_path=".", top_n_regions=5):
    """Analyzes actor-director collaborations by region."""
    os.makedirs(save_path, exist_ok=True)
    title_principals = dataframes["title.principals"]
    title_crew = dataframes["title.crew"]
    title_ratings = dataframes["title.ratings"]
    name_basics = dataframes["name.basics"]
    title_akas = dataframes["title.akas"]
    title_basics = dataframes["title.basics"]

    # Get top regions
    top_regions = get_top_regions(title_akas, top_n_regions)
    print(f"Analyzing actor-director collaborations for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        title_akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter for movies only
    title_basics_movies = title_basics.filter(F.col("titleType") == "movie")
    
    # Filter principals and crew by filtered basics
    title_principals_filtered = title_principals.join(title_basics_movies.select("tconst"), "tconst")
    title_crew_filtered = title_crew.join(title_basics_movies.select("tconst"), "tconst")

    actors = title_principals_filtered.filter(F.col("category").isin(["actor", "actress"])) \
        .select(F.col("tconst"), F.col("nconst").alias("actor_nconst"))

    directors = title_crew_filtered.filter(F.col("directors").isNotNull()) \
        .withColumn("director_nconst", F.explode(F.split(F.col("directors"), ","))) \
        .select("tconst", "director_nconst")

    collaborations = actors.join(directors, "tconst").join(regional_akas, "tconst")

    actor_names = name_basics.select(F.col("nconst").alias("actor_nconst"), F.col("primaryName").alias("actor_name"))
    director_names = name_basics.select(F.col("nconst").alias("director_nconst"), F.col("primaryName").alias("director_name"))

    collaborations_with_names = collaborations.join(actor_names, "actor_nconst") \
        .join(director_names, "director_nconst")

    collaboration_counts = (
        collaborations_with_names.groupBy("region", "actor_name", "director_name")
        .agg(F.count("tconst").alias("count"))
    )

    # Get top collaborations per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("count"))
    top_collabs = (
        collaboration_counts.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= 10)
        .orderBy("region", "rank")
    )

    result_pd = top_collabs.toPandas()

    if not result_pd.empty:
        result_pd["collaboration"] = result_pd["director_name"] + " - " + result_pd["actor_name"]

        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(10)

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="count",
                    y="collaboration",
                    hue="collaboration",
                    palette="crest",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Top Actor-Director Pairs in {region}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Number of Collaborations", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Actor-Director Collaborations in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "actor_director_collaborations_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_collabs
