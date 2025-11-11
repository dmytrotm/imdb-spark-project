"""
Film genre analysis with popularity and engagement metrics
"""
from pyspark.sql import functions as F


def top_genres_after_2010(datasets):
    """
    BUSINESS QUESTION 1: What are the top 5 most popular genres by engagement 
    among feature films released after 2010?
    
    Goal: Identify not just the number of films, but the real popularity of genres through 
    audience engagement (number of votes) and quality (rating). This provides a more accurate 
    picture for investment decisions - which genres are not only popular for production, 
    but also actually attract audiences and receive high ratings.
    
    Metrics:
    - Number of films in the genre
    - Average genre rating
    - Total number of votes (popularity)
    - Average votes per film (engagement)
    
    Operations: Filtering, Joining with ratings, Grouping, Aggregation
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 1: Top-5 genres by popularity and engagement after 2010")
    print("="*80)
    
    basics_df = datasets['title.basics']
    ratings_df = datasets['title.ratings']
    
    # Filter feature films after 2010
    films_after_2010 = basics_df.filter(
        (F.col("titleType") == "movie") & 
        (F.col("startYear") > 2010) &
        (F.col("genres") != "\\N")
    )
    
    # Join with ratings
    films_with_ratings = films_after_2010.join(
        ratings_df,
        films_after_2010.tconst == ratings_df.tconst,
        "inner"
    ).drop(ratings_df.tconst)
    
    # Explode genres
    genres_exploded = films_with_ratings.select(
        F.explode(F.split(F.col("genres"), ",")).alias("genre"),
        "averageRating",
        "numVotes"
    )
    
    # Group and calculate metrics
    genre_stats = genres_exploded.groupBy("genre") \
        .agg(
            F.count("*").alias("film_count"),
            F.avg("averageRating").alias("avg_rating"),
            F.sum("numVotes").alias("total_votes"),
            F.avg("numVotes").alias("avg_votes_per_film")
        )
    
    # Create composite score for ranking
    # Score = (normalized votes) * (normalized rating) * sqrt(film_count)
    genre_stats = genre_stats.withColumn(
        "engagement_score",
        (F.col("avg_votes_per_film") / F.lit(1000.0)) * 
        (F.col("avg_rating") / F.lit(10.0)) * 
        F.sqrt(F.col("film_count"))
    )
    
    top_genres = genre_stats.orderBy(F.desc("engagement_score")).limit(5)
    
    print("\nTop-5 genres by engagement (popularity × quality × scale):")
    top_genres.select(
        "genre", 
        "film_count",
        F.round("avg_rating", 2).alias("avg_rating"),
        "total_votes",
        F.round("avg_votes_per_film", 0).alias("avg_votes_per_film"),
        F.round("engagement_score", 2).alias("engagement_score")
    ).show(truncate=False)
    
    return top_genres

