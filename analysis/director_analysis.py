
import os
from datetime import datetime

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
    os.makedirs(save_path, exist_ok=True)
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
    plt.legend(title="Director", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, "director_ratings_trend.png"))
    plt.show()

    return director_trend_count


def top_directors_by_high_rating_and_votes(dataframes, save_path="."):
    os.makedirs(save_path, exist_ok=True)
    title_crew = dataframes["title.crew"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"]
    name_basics = dataframes["name.basics"]

    directors = title_crew.withColumn("nconst", F.explode(F.split(F.col("directors"), ",")))

    directors_rated = directors.join(ratings, "tconst") \
                               .join(basics.select("tconst", "startYear"), "tconst") \
                               .join(name_basics.select("nconst", "primaryName"), "nconst")

    directors_rated = directors_rated.filter(F.col("averageRating") > 8)

    yearly_stats = directors_rated.groupBy("primaryName", "startYear").agg(
        F.count("tconst").alias("num_films"),
        F.avg("numVotes").alias("avg_votes")
    ).orderBy("primaryName", "startYear")
    
    # Find top directors by total number of high-rated films
    top_directors_by_count = directors_rated.groupBy("primaryName").count().orderBy(F.desc("count")).limit(10)
    
    # Filter yearly stats for top directors
    top_directors_trends = yearly_stats.join(top_directors_by_count.select("primaryName"), "primaryName")
    
    trends_pd = top_directors_trends.toPandas()

    if not trends_pd.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=trends_pd, x="startYear", y="avg_votes", hue="primaryName", marker="o")
        plt.title("Popularity Trend (Avg. Votes) for Directors with Most High-Rated (>8) Films")
        plt.xlabel("Year")
        plt.ylabel("Average Number of Votes")
        plt.legend(title="Director", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "top_directors_votes_trend.png"))
        plt.show()

    return yearly_stats


def director_career_span_analysis(dataframes, save_path=".", min_career_length=20, min_film_count=10, num_directors_to_plot=7):
    """
    Analyzes the evolution of director ratings over their entire career span.
    """
    os.makedirs(save_path, exist_ok=True)
    title_crew = dataframes["title.crew"]
    title_basics = dataframes["title.basics"]
    title_ratings = dataframes["title.ratings"]
    name_basics = dataframes["name.basics"]

    # Get director films
    director_films = title_crew.withColumn("nconst", F.explode(F.split(F.col("directors"), ","))) \
        .join(name_basics.select("nconst", "primaryName"), "nconst") \
        .join(title_basics.filter(F.col("titleType") == "movie").select("tconst", "startYear"), "tconst")

    # Find first year for each director
    first_year = director_films.groupBy("primaryName").agg(F.min("startYear").alias("first_film_year"))

    # Calculate career year and join ratings
    director_career_data = director_films.join(first_year, "primaryName") \
        .withColumn("career_year", F.col("startYear") - F.col("first_film_year")) \
        .join(title_ratings, "tconst")

    # Find prolific directors with long careers
    career_stats = director_career_data.groupBy("primaryName").agg(
        (F.max("startYear") - F.min("startYear")).alias("career_length"),
        F.count("tconst").alias("film_count")
    )
    
    prolific_directors = career_stats.filter((F.col("career_length") >= min_career_length) & (F.col("film_count") > min_film_count)) \
        .orderBy(F.desc("film_count")).limit(num_directors_to_plot)

    # Filter for these directors and get yearly average rating
    top_directors_career_ratings = director_career_data.join(prolific_directors.select("primaryName"), "primaryName") \
        .groupBy("primaryName", "career_year") \
        .agg(F.avg("averageRating").alias("avg_rating")) \
        .orderBy("primaryName", "career_year")

    # Visualization
    career_ratings_pd = top_directors_career_ratings.toPandas()

    if not career_ratings_pd.empty:
        plt.figure(figsize=(15, 8))
        sns.lineplot(data=career_ratings_pd, x="career_year", y="avg_rating", hue="primaryName", marker="o")
        plt.title("Rating Evolution Over a Director's Career")
        plt.xlabel("Years Into Career")
        plt.ylabel("Average Film Rating")
        plt.legend(title="Director", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "director_career_span.png"))
        plt.show()
