"""
Director analysis and their films
"""
from pyspark.sql import functions as F


def top_directors_high_rated_films(datasets):
    """
    BUSINESS QUESTION 6: Which 3 directors have the highest number of films 
    with a rating above 8.0 and runtime longer than 90 minutes?
    
    Goal: Identify directors who consistently create highly-rated feature films. 
    This information is important for studios looking for successful directors for 
    their projects, for festivals when forming programs, and for film critics 
    and audiences when choosing quality films to watch.
    
    Operations: Joining (title_basics, title_crew, title_ratings, name_basics), 
                Filtering (titleType, averageRating > 8.0, runtimeMinutes > 90), 
                Grouping (directors)
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 6: Top-3 directors with most high-rated films")
    print("="*80)
    
    basics_df = datasets['title.basics']
    crew_df = datasets['title.crew']
    ratings_df = datasets['title.ratings']
    names_df = datasets['name.basics']
    
    # Filter films with runtime longer than 90 minutes
    long_movies = basics_df.filter(
        (F.col("titleType") == "movie") &
        (F.col("runtimeMinutes") > 90)
    )
    
    # Join with ratings and filter by rating
    high_rated_movies = long_movies.join(
        ratings_df,
        long_movies.tconst == ratings_df.tconst,
        "inner"
    ).filter(F.col("averageRating") > 8.0).drop(ratings_df.tconst)
    
    # Join with crew to get directors
    movies_with_directors = high_rated_movies.join(
        crew_df,
        high_rated_movies.tconst == crew_df.tconst,
        "inner"
    ).drop(crew_df.tconst)
    
    # Explode directors list (they can be comma-separated)
    directors_exploded = movies_with_directors.select(
        F.explode(F.split(F.col("directors"), ",")).alias("director_nconst"),
        "primaryTitle",
        "averageRating",
        "runtimeMinutes",
        "numVotes"
    ).filter(F.col("director_nconst") != "\\N")
    
    # Group by directors
    director_stats = directors_exploded.groupBy("director_nconst") \
        .agg(
            F.count("*").alias("high_rated_count"),
            F.avg("averageRating").alias("avg_rating")
        ) \
        .orderBy(F.desc("high_rated_count")) \
        .limit(3)
    
    # Add director names
    top_directors = director_stats.join(
        names_df.select("nconst", "primaryName"),
        director_stats.director_nconst == names_df.nconst,
        "inner"
    )
    
    print("\nTop-3 directors:")
    top_directors.select(
        "primaryName", "high_rated_count", "avg_rating"
    ).show(truncate=False)
    
    # Show some films from these directors
    print("\nExamples of high-rated films from these directors:")
    top_director_ids = [row['director_nconst'] for row in director_stats.collect()]
    
    for director_id in top_director_ids:
        director_name = names_df.filter(F.col("nconst") == director_id) \
            .select("primaryName").first()["primaryName"]
        
        print(f"\n{director_name}:")
        directors_exploded.filter(F.col("director_nconst") == director_id) \
            .select("primaryTitle", "averageRating", "runtimeMinutes", "numVotes") \
            .orderBy(F.desc("averageRating")) \
            .limit(5) \
            .show(truncate=False)
    
    return top_directors

