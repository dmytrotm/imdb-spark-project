
import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def directors_increasing_ratings_trend(dataframes, save_path="."):
    """
    Analyzes the trend of directors' average film ratings over the past 10 years.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
    Returns:
        pyspark.sql.DataFrame: A DataFrame showing directors with an increasing ratings trend.
    """
    # Get the current year
    from datetime import datetime
    current_year = datetime.now().year
    last_10_years = current_year - 10

    # Join dataframes
    title_basics = dataframes["title.basics"]
    title_ratings = dataframes["title.ratings"]
    title_crew = dataframes["title.crew"]
    name_basics = dataframes["name.basics"]

    # Filter for movies in the last 10 years
    movies = title_basics.filter(
        (F.col("titleType") == "movie") & (F.col("startYear") >= last_10_years)
    )

    # Join with ratings
    movies_with_ratings = movies.join(title_ratings, "tconst")

    # Join with crew to get directors
    movies_with_crew = movies_with_ratings.join(title_crew, "tconst")

    # Explode directors array
    movies_with_directors = movies_with_crew.withColumn(
        "director_nconst", F.explode(F.split(F.col("directors"), ","))
    )

    # Join with name_basics to get director names
    movies_with_director_names = movies_with_directors.join(
        name_basics, movies_with_directors.director_nconst == name_basics.nconst
    )

    # Group by director and year to get average rating
    director_yearly_ratings = (
        movies_with_director_names.groupBy("primaryName", "startYear")
        .agg(F.avg("averageRating").alias("avg_rating"))
        .orderBy("primaryName", "startYear")
    )

    # Window function to compare with previous year's rating
    window_spec = Window.partitionBy("primaryName").orderBy("startYear")
    ratings_trend = director_yearly_ratings.withColumn(
        "prev_year_rating", F.lag("avg_rating").over(window_spec)
    )

    # Filter for directors with increasing trend
    increasing_trend = ratings_trend.filter(
        F.col("avg_rating") > F.col("prev_year_rating")
    )

    # Count the number of years with an increasing trend for each director
    director_trend_count = (
        increasing_trend.groupBy("primaryName")
        .count()
        .orderBy(F.desc("count"))
    )

    # Get the detailed trend for the top directors
    top_directors = director_trend_count.limit(10)
    top_directors_trend = director_yearly_ratings.join(
        top_directors, "primaryName"
    ).orderBy("primaryName", "startYear")

    # Visualization
    top_directors_trend_pd = top_directors_trend.toPandas()

    plt.figure(figsize=(14, 7))
    sns.lineplot(
        data=top_directors_trend_pd,
        x="startYear",
        y="avg_rating",
        hue="primaryName",
        marker="o",
    )
    plt.title("Average Film Ratings Trend for Top Directors (Last 10 Years)")
    plt.xlabel("Year")
    plt.ylabel("Average Rating")
    plt.legend(title="Director")
    plt.grid(True)
    plt.savefig(os.path.join(save_path, "director_ratings_trend.png"))
    plt.show()

    return director_trend_count
