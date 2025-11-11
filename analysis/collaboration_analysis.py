"""
Collaboration analysis: actors, their ratings, and young talents
"""
from pyspark.sql import functions as F


def avg_rating_by_actor(datasets):
    """
    BUSINESS QUESTION 4: For each actor who appeared in at least 10 films, 
    what is the average rating of those films?
    
    Goal: Identify actors whose participation is statistically associated with high 
    film ratings. This information is valuable for casting directors, producers, and directors 
    when choosing actors for new projects, as well as for actors' agents for 
    positioning their clients.
    
    Operations: Joining (title_basics, title_principals, title_ratings, name_basics), 
                Filtering (titleType, actor category, minimum 10 films), 
                Grouping (nconst)
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 4: Actors with highest average film rating (min. 10 films)")
    print("="*80)
    
    basics_df = datasets['title.basics']
    principals_df = datasets['title.principals']
    ratings_df = datasets['title.ratings']
    names_df = datasets['name.basics']
    
    # Filter only movies
    movies = basics_df.filter(F.col("titleType") == "movie")
    
    # Filter only actors and actresses
    actors = principals_df.filter(
        (F.col("category") == "actor") | (F.col("category") == "actress")
    )
    
    # Join movies with actors
    movies_actors = movies.join(
        actors,
        movies.tconst == actors.tconst,
        "inner"
    ).drop(actors.tconst)
    
    # Join with ratings
    movies_actors_ratings = movies_actors.join(
        ratings_df,
        movies_actors.tconst == ratings_df.tconst,
        "inner"
    ).drop(ratings_df.tconst)
    
    # Group by actors and calculate average rating
    actor_stats = movies_actors_ratings.groupBy("nconst") \
        .agg(
            F.avg("averageRating").alias("avg_rating"),
            F.count("*").alias("film_count")
        ) \
        .filter(F.col("film_count") >= 10)
    
    # Add actor names
    actor_stats_with_names = actor_stats.join(
        names_df.select("nconst", "primaryName"),
        "nconst",
        "inner"
    )
    
    print("\nTop-20 actors with highest average rating:")
    actor_stats_with_names.select(
        "primaryName", "film_count", "avg_rating"
    ).orderBy(F.desc("avg_rating")).limit(20).show(truncate=False)
    
    return actor_stats_with_names


def young_actors_2000s(datasets):
    """
    BUSINESS QUESTION 5: What is the distribution of actors by age groups 
    in films from the last 5 years?
    
    Goal: Identify which age groups of actors are most in demand in modern cinema. 
    This helps casting directors and producers understand trends, studios plan 
    projects for target audiences, and agents position their clients.
    
    Age groups:
    - 18-25: Youth, newcomers
    - 26-35: Prime time, peak demand
    - 36-45: Mature actors, complex roles
    - 46-55: Experienced professionals
    - 56+: Industry veterans
    
    Operations: Joining (title_basics, title_principals, name_basics), 
                Filtering (last 5 years), Age calculation, Grouping
    """
    print("\n" + "="*80)
    print("BUSINESS QUESTION 5: Actor age distribution in films 2019-2024")
    print("="*80)
    
    from datetime import datetime
    current_year = datetime.now().year
    
    basics_df = datasets['title.basics']
    principals_df = datasets['title.principals']
    names_df = datasets['name.basics']
    
    # Filter films from the last 5 years
    recent_movies = basics_df.filter(
        (F.col("titleType") == "movie") &
        (F.col("startYear") >= current_year - 5) &
        (F.col("startYear") <= current_year)
    )
    
    # Filter actors/actresses
    actors = principals_df.filter(
        (F.col("category") == "actor") | (F.col("category") == "actress")
    )
    
    # Join films with actors
    movies_actors = recent_movies.join(
        actors,
        recent_movies.tconst == actors.tconst,
        "inner"
    ).drop(actors.tconst)
    
    # Join with actor information
    actors_info = movies_actors.join(
        names_df.filter(F.col("birthYear").isNotNull()),
        "nconst",
        "inner"
    )
    
    # Calculate actor's age at film release
    actors_with_age = actors_info.withColumn(
        "age_at_release",
        F.col("startYear") - F.col("birthYear")
    ).filter((F.col("age_at_release") >= 18) & (F.col("age_at_release") <= 100))
    
    # Create age groups
    actors_with_age = actors_with_age.withColumn(
        "age_group",
        F.when((F.col("age_at_release") >= 18) & (F.col("age_at_release") <= 25), "18-25 (Youth)")
         .when((F.col("age_at_release") >= 26) & (F.col("age_at_release") <= 35), "26-35 (Prime)")
         .when((F.col("age_at_release") >= 36) & (F.col("age_at_release") <= 45), "36-45 (Mature)")
         .when((F.col("age_at_release") >= 46) & (F.col("age_at_release") <= 55), "46-55 (Experienced)")
         .otherwise("56+ (Veterans)")
    )
    
    # Group by age categories
    age_distribution = actors_with_age.groupBy("age_group") \
        .agg(
            F.countDistinct("nconst").alias("unique_actors"),
            F.count("*").alias("total_roles"),
            F.avg("age_at_release").alias("avg_age")
        ) \
        .orderBy("avg_age")
    
    print("\nActor distribution by age groups in films 2019-2024:")
    age_distribution.select(
        "age_group",
        "unique_actors",
        "total_roles",
        F.round("avg_age", 1).alias("avg_age")
    ).show(truncate=False)
    
    # Additional: trend by years
    print("\nTrend by years (average actor age):")
    yearly_trend = actors_with_age.groupBy("startYear") \
        .agg(
            F.avg("age_at_release").alias("avg_age"),
            F.countDistinct("nconst").alias("unique_actors")
        ) \
        .orderBy("startYear")
    
    yearly_trend.select(
        "startYear",
        F.round("avg_age", 1).alias("avg_age"),
        "unique_actors"
    ).show()
    
    # Top-10 youngest actors in recent films
    print("\nTop-10 youngest actors in films from last 5 years:")
    actors_with_age.select(
        "primaryName",
        "birthYear",
        "startYear",
        "age_at_release",
        "primaryTitle"
    ).orderBy("age_at_release").limit(10).show(truncate=False)
    
    return age_distribution

