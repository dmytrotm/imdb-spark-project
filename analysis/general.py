"""
General IMDB data analysis
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def avg_rating_long_series(datasets):
    """
    BUSINESS QUESTION 2: What is the average rating of TV series with more than 50 episodes?
    
    Goal: Provide insight into how audiences rate long-running TV series. 
    This is important for TV networks and streaming services to evaluate project success 
    and make decisions about extending series or creating new similar formats.
    
    Operations: Joining (title_basics, title_episode, title_ratings), 
                Filtering (titleType, episode count)
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 2: Average rating of series with 50+ episodes")
    print("="*80)
    
    basics_df = datasets['title.basics']
    episodes_df = datasets['title.episode']
    ratings_df = datasets['title.ratings']
    
    # Count episodes for each series
    episode_counts = episodes_df.groupBy("parentTconst") \
        .agg(F.count("*").alias("episode_count"))
    
    # Filter series with more than 50 episodes
    long_series = episode_counts.filter(F.col("episode_count") > 50)
    
    # Join with basics to get series information
    series_info = long_series.join(
        basics_df.filter(F.col("titleType") == "tvSeries"),
        long_series.parentTconst == basics_df.tconst,
        "inner"
    )
    
    # Join with ratings
    series_with_ratings = series_info.join(
        ratings_df,
        series_info.tconst == ratings_df.tconst,
        "inner"
    )
    
    # Calculate average rating
    avg_rating = series_with_ratings.agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("*").alias("series_count")
    )
    
    print(f"\nSeries with 50+ episodes:")
    print(f"Series count: {avg_rating.first()['series_count']}")
    print(f"Average rating: {avg_rating.first()['avg_rating']:.2f}")
    
    # Show top-10 long series with highest ratings
    print("\nTop-10 long series with highest ratings:")
    series_with_ratings.select(
        "primaryTitle", "episode_count", "averageRating", "numVotes"
    ).orderBy(F.desc("averageRating")).limit(10).show(truncate=False)
    
    return avg_rating


def season_rating_diff(datasets):
    """
    BUSINESS QUESTION 3: For each TV series with more than 3 seasons, determine 
    the difference in average rating between the current season and the previous one.
    
    Goal: Track the dynamics of audience rating changes from season to season. 
    Rating growth may indicate quality improvement or audience growth, 
    decline - loss of interest. This information is critical for showrunners, 
    writers, and producers to adjust the production process.
    
    Operations: Filtering (titleType, season count), 
                Window functions (lag to compare with previous season)
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 3: Season rating dynamics for long series")
    print("="*80)
    
    episodes_df = datasets['title.episode']
    ratings_df = datasets['title.ratings']
    basics_df = datasets['title.basics']
    
    # Join episodes with ratings
    episodes_with_ratings = episodes_df.join(
        ratings_df,
        episodes_df.tconst == ratings_df.tconst,
        "inner"
    )
    
    # Calculate average rating by seasons
    season_ratings = episodes_with_ratings.groupBy("parentTconst", "seasonNumber") \
        .agg(F.avg("averageRating").alias("season_avg_rating"))
    
    # Count number of seasons
    season_counts = season_ratings.groupBy("parentTconst") \
        .agg(F.max("seasonNumber").alias("max_season"))
    
    # Filter series with more than 3 seasons
    long_series = season_counts.filter(F.col("max_season") > 3)
    
    # Filter ratings only for long series
    long_series_ratings = season_ratings.join(
        long_series,
        "parentTconst",
        "inner"
    )
    
    # Use window function to get previous season's rating
    window_spec = Window.partitionBy("parentTconst").orderBy("seasonNumber")
    
    ratings_with_prev = long_series_ratings.withColumn(
        "prev_season_rating",
        F.lag("season_avg_rating", 1).over(window_spec)
    ).withColumn(
        "rating_diff",
        F.col("season_avg_rating") - F.col("prev_season_rating")
    )
    
    # Add series titles
    result = ratings_with_prev.join(
        basics_df.select("tconst", "primaryTitle"),
        ratings_with_prev.parentTconst == basics_df.tconst,
        "inner"
    )
    
    print("\nSeries with highest rating growth between seasons:")
    result.filter(F.col("rating_diff").isNotNull()) \
        .select("primaryTitle", "seasonNumber", "season_avg_rating", 
                "prev_season_rating", "rating_diff") \
        .orderBy(F.desc("rating_diff")) \
        .limit(10) \
        .show(truncate=False)
    
    print("\nSeries with biggest rating drop between seasons:")
    result.filter(F.col("rating_diff").isNotNull()) \
        .select("primaryTitle", "seasonNumber", "season_avg_rating", 
                "prev_season_rating", "rating_diff") \
        .orderBy(F.asc("rating_diff")) \
        .limit(10) \
        .show(truncate=False)
    
    return result

