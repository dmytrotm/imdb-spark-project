import os
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def genre_popularity_trend(dataframes, save_path=".", top_n_regions=5, last_n_years=10):
    """
    Analyzes the trend of genre popularity based on the number of votes and average rating over the past N years.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        top_n_regions (int): Number of top regions to analyze. Defaults to 5.
        last_n_years (int): Number of years to analyze. Defaults to 10.
    Returns:
        pyspark.sql.DataFrame: A DataFrame showing genres with increasing popularity.
    """
    current_year = datetime.now().year
    start_year = current_year - last_n_years

    title_basics = dataframes["title.basics"]
    title_ratings = dataframes["title.ratings"]
    title_akas = dataframes["title.akas"]

    # Filter movies by year range
    movies = title_basics.filter(
        (F.col("titleType") == "movie")
        & (F.col("startYear") >= start_year)
        & (F.col("startYear") <= current_year)
    )

    print(
        f"Analyzing movies from {start_year} to {current_year} ({last_n_years} years)"
    )

    movies_with_ratings = movies.join(title_ratings, "tconst")
    movies_with_akas = movies_with_ratings.join(
        title_akas, movies_with_ratings.tconst == title_akas.titleId
    )

    genre_trends = movies_with_akas.withColumn(
        "genre", F.explode(F.split(F.col("genres"), ","))
    ).filter(F.col("region") != "\\N")

    # Calculate top regions by number of movies
    region_counts = (
        genre_trends.groupBy("region")
        .count()
        .orderBy(F.col("count").desc())
        .limit(top_n_regions)
    )

    top_regions = [row.region for row in region_counts.collect()]

    print(f"Analyzing top {len(top_regions)} regions: {', '.join(top_regions)}")

    genre_yearly_stats = (
        genre_trends.groupBy("region", "genre", "startYear")
        .agg(
            F.avg("numVotes").alias("avg_votes"),
            F.avg("averageRating").alias("avg_rating"),
        )
        .orderBy("region", "genre", "startYear")
    )

    window_spec = Window.partitionBy("region", "genre").orderBy("startYear")
    genre_trend_analysis = genre_yearly_stats.withColumn(
        "prev_year_votes", F.lag("avg_votes").over(window_spec)
    ).withColumn("prev_year_rating", F.lag("avg_rating").over(window_spec))

    increasing_genres = genre_trend_analysis.filter(
        (F.col("avg_votes") > F.col("prev_year_votes"))
        & (F.col("avg_rating") > F.col("prev_year_rating"))
    )

    top_genres_in_regions = increasing_genres.filter(
        F.col("region").isin(top_regions)
    ).toPandas()

    if not top_genres_in_regions.empty:
        # Adjust col_wrap based on number of regions
        col_wrap = min(3, len(top_regions))

        g = sns.FacetGrid(
            top_genres_in_regions,
            col="region",
            hue="genre",
            col_wrap=col_wrap,
            sharey=False,
        )
        g.map(sns.lineplot, "startYear", "avg_votes", marker="o")
        g.add_legend()
        g.fig.suptitle(
            f"Genre Popularity Trend (Avg. Votes) - Last {last_n_years} Years", y=1.03
        )
        g.set_axis_labels("Year", "Average Votes")
        plt.savefig(os.path.join(save_path, "genre_votes_trend.png"))
        plt.show()

        g = sns.FacetGrid(
            top_genres_in_regions,
            col="region",
            hue="genre",
            col_wrap=col_wrap,
            sharey=False,
        )
        g.map(sns.lineplot, "startYear", "avg_rating", marker="o")
        g.add_legend()
        g.figure.suptitle(
            f"Genre Popularity Trend (Avg. Rating) - Last {last_n_years} Years", y=1.03
        )
        g.set_axis_labels("Year", "Average Rating")
        plt.savefig(os.path.join(save_path, "genre_rating_trend.png"))
        plt.show()

    return increasing_genres


def genre_actor_cyclicality(
    dataframes, save_path=".", top_n_regions=5, year_range=None
):
    """
    Analyzes the cyclicality in the popularity of genres and actors in different countries.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        top_n_regions (int): Number of top regions to analyze. Defaults to 5.
        year_range (tuple): Optional tuple of (start_year, end_year) to filter data.
                           If None, analyzes all available years.
    """
    title_basics = dataframes["title.basics"]
    title_principals = dataframes["title.principals"]
    name_basics = dataframes["name.basics"]
    title_akas = dataframes["title.akas"]

    movies = title_basics.filter(F.col("titleType") == "movie")

    # Apply year filter if specified
    if year_range:
        start_year, end_year = year_range
        movies = movies.filter(
            (F.col("startYear") >= start_year) & (F.col("startYear") <= end_year)
        )
        print(f"Analyzing movies from {start_year} to {end_year}")
    else:
        print("Analyzing movies from all available years")

    movie_actors = movies.join(title_principals, "tconst").filter(
        F.col("category").isin(["actor", "actress"])
    )
    movie_actors_names = movie_actors.join(name_basics, "nconst")
    movie_actors_regions = movie_actors_names.join(
        title_akas, movie_actors_names.tconst == title_akas.titleId
    ).filter(F.col("region") != "\\N")

    # Calculate top regions by number of actor appearances
    region_counts = (
        movie_actors_regions.groupBy("region")
        .count()
        .orderBy(F.col("count").desc())
        .limit(top_n_regions)
    )

    top_regions = [row.region for row in region_counts.collect()]

    print(f"Analyzing top {len(top_regions)} regions: {', '.join(top_regions)}")

    genre_popularity = (
        movie_actors_regions.withColumn(
            "genre", F.explode(F.split(F.col("genres"), ","))
        )
        .groupBy("region", "genre", "startYear")
        .count()
        .orderBy("count", ascending=False)
    )

    actor_popularity = (
        movie_actors_regions.groupBy("region", "primaryName", "startYear")
        .count()
        .orderBy("count", ascending=False)
    )

    top_genres_pd = (
        genre_popularity.filter(F.col("region").isin(top_regions)).limit(20).toPandas()
    )
    top_actors_pd = (
        actor_popularity.filter(F.col("region").isin(top_regions)).limit(20).toPandas()
    )

    # Add year range to title if specified
    year_suffix = f" ({year_range[0]}-{year_range[1]})" if year_range else ""

    if not top_genres_pd.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(
            data=top_genres_pd,
            x="startYear",
            y="count",
            hue="genre",
            style="region",
            marker="o",
        )
        plt.title(
            f"Cyclicality of Genre Popularity in Different Countries{year_suffix}"
        )
        plt.xlabel("Year")
        plt.ylabel("Number of Movies")
        plt.legend(title="Genre/Region")
        plt.grid(True)
        plt.savefig(os.path.join(save_path, "genre_cyclicality.png"))
        plt.show()

    if not top_actors_pd.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(
            data=top_actors_pd,
            x="startYear",
            y="count",
            hue="primaryName",
            style="region",
            marker="o",
        )
        plt.title(
            f"Cyclicality of Actor Popularity in Different Countries{year_suffix}"
        )
        plt.xlabel("Year")
        plt.ylabel("Number of Movies")
        plt.legend(title="Actor/Region")
        plt.grid(True)
        plt.savefig(os.path.join(save_path, "actor_cyclicality.png"))
        plt.show()


def genre_duration_rating_analysis(dataframes, save_path=".", year_range=None):
    """
    Analyzes the average duration of films in different genres and its effect on rating.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        year_range (tuple): Optional tuple of (start_year, end_year) to filter data.
                           If None, analyzes all available years.
    """
    title_basics = dataframes["title.basics"]
    title_ratings = dataframes["title.ratings"]

    movies = title_basics.filter(F.col("titleType") == "movie")

    # Apply year filter if specified
    if year_range:
        start_year, end_year = year_range
        movies = movies.filter(
            (F.col("startYear") >= start_year) & (F.col("startYear") <= end_year)
        )
        print(f"Analyzing movies from {start_year} to {end_year}")
        year_suffix = f" ({start_year}-{end_year})"
    else:
        print("Analyzing movies from all available years")
        year_suffix = ""

    movies_with_ratings = movies.join(title_ratings, "tconst")

    genre_analysis = (
        movies_with_ratings.withColumn(
            "genre", F.explode(F.split(F.col("genres"), ","))
        )
        .groupBy("genre")
        .agg(
            F.avg("runtimeMinutes").alias("avg_runtime"),
            F.avg("averageRating").alias("avg_rating"),
            F.count("*").alias("movie_count"),
        )
        .orderBy("genre")
    )

    genre_analysis_pd = genre_analysis.toPandas()

    if not genre_analysis_pd.empty:
        plt.figure(figsize=(14, 8))
        sns.scatterplot(
            data=genre_analysis_pd,
            x="avg_runtime",
            y="avg_rating",
            size="movie_count",
            hue="genre",
            sizes=(50, 500),
            alpha=0.7,
        )
        plt.title(f"Average Duration vs. Average Rating by Genre{year_suffix}")
        plt.xlabel("Average Runtime (Minutes)")
        plt.ylabel("Average Rating")
        plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "genre_duration_rating.png"))
        plt.show()

    return genre_analysis
