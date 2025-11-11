import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F


def actors_demography_stats(dataframes, save_path="."):
    """
    Соціальна демографія акторів: вік (birthYear), активність (уникальна кількість фільмів), середній рейтинг
    """
    os.makedirs(save_path, exist_ok=True)
    # Таблиці
    name_basics = dataframes["name.basics"]
    principals = dataframes["title.principals"]
    ratings = dataframes["title.ratings"]

    # Беремо тільки акторів та актрис
    actors = principals.filter(F.col("category").isin(["actor", "actress"]))

    # Обчислюємо унікальні пари актор-фільм (щоб не рахувати дублікати)
    distinct_works = actors.select("nconst", "tconst").distinct()

    # Підрахунок унікальної кількості фільмів/серіалів на актора
    film_counts = distinct_works.groupBy("nconst").agg(F.count("tconst").alias("num_titles"))

    # Об’єднуємо з інформацією про акторів (ім'я та рік народження)
    actors_info = film_counts.join(
        name_basics.select("nconst", "primaryName", "birthYear"),
        "nconst"
    )

    # Обчислюємо середній рейтинг для кожного актора
    actors_films = distinct_works.join(ratings, "tconst")
    avg_ratings = actors_films.groupBy("nconst").agg(F.avg("averageRating").alias("avg_rating"))

    # Об’єднуємо всі дані
    actors_info = actors_info.join(avg_ratings, "nconst")

    # Відкидаємо дублі, сортуємо за активністю та обираємо топ 30
    result = actors_info.dropDuplicates().orderBy(F.desc("num_titles")).limit(30)

    result_pd = result.toPandas()

    if not result_pd.empty:
        plt.figure(figsize=(14, 8))
        sns.scatterplot(data=result_pd, x="num_titles", y="avg_rating", hue="birthYear", size="num_titles", sizes=(100, 1000), alpha=0.7, palette="viridis")
        plt.title("Top 30 Actors: Career Volume vs. Average Rating")
        plt.xlabel("Number of Titles")
        plt.ylabel("Average Rating")
        plt.legend(title="Birth Year", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        
        # Add labels for each point
        for i, row in result_pd.iterrows():
            plt.text(row['num_titles'] + 0.3, row['avg_rating'], row['primaryName'], fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "actor_stats_scatterplot.png"))
        plt.show()

    return result


def avg_rating_by_actor(datasets, save_path=".", min_films=10, top_n=20):
    """
    Analyzes average film ratings for actors with a minimum number of films.
    Identifies actors associated with high-quality productions for casting decisions.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        min_films (int): Minimum number of films. Defaults to 10.
        top_n (int): Number of top actors to show. Defaults to 20.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with actor statistics including average ratings and film counts.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Actors with highest average film rating (min. {min_films} films)")
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
            F.count("*").alias("film_count"),
            F.sum("numVotes").alias("total_votes")
        ) \
        .filter(F.col("film_count") >= min_films)
    
    # Add actor names
    actor_stats_with_names = actor_stats.join(
        names_df.select("nconst", "primaryName", "birthYear"),
        "nconst",
        "inner"
    )
    
    print(f"\nTop-{top_n} actors with highest average rating:")
    top_actors = actor_stats_with_names.orderBy(F.desc("avg_rating")).limit(top_n)
    top_actors.select(
        "primaryName", "film_count", F.round("avg_rating", 2).alias("avg_rating"),
        "total_votes", "birthYear"
    ).show(truncate=False)
    
    # Visualization
    top_actors_pd = top_actors.toPandas()
    
    if not top_actors_pd.empty:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Top actors by average rating
        axes[0, 0].barh(range(len(top_actors_pd)), top_actors_pd['avg_rating'])
        axes[0, 0].set_yticks(range(len(top_actors_pd)))
        axes[0, 0].set_yticklabels(top_actors_pd['primaryName'], fontsize=9)
        axes[0, 0].set_title(f'Top {top_n} Actors by Average Rating (min. {min_films} films)')
        axes[0, 0].set_xlabel('Average Rating')
        axes[0, 0].grid(True, alpha=0.3, axis='x')
        axes[0, 0].invert_yaxis()
        
        # 2. Film count vs Average rating
        axes[0, 1].scatter(top_actors_pd['film_count'], top_actors_pd['avg_rating'], 
                          s=100, alpha=0.6, color='steelblue')
        axes[0, 1].set_title('Film Count vs Average Rating')
        axes[0, 1].set_xlabel('Number of Films')
        axes[0, 1].set_ylabel('Average Rating')
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. Total votes distribution
        axes[1, 0].bar(range(len(top_actors_pd)), top_actors_pd['total_votes'], color='coral')
        axes[1, 0].set_xticks(range(len(top_actors_pd)))
        axes[1, 0].set_xticklabels(top_actors_pd['primaryName'], rotation=90, fontsize=8)
        axes[1, 0].set_title('Total Votes (Popularity)')
        axes[1, 0].set_ylabel('Total Votes')
        axes[1, 0].grid(True, alpha=0.3, axis='y')
        
        # 4. Birth year distribution
        if 'birthYear' in top_actors_pd.columns:
            birth_year_clean = top_actors_pd['birthYear'].dropna()
            if len(birth_year_clean) > 0:
                axes[1, 1].hist(birth_year_clean, bins=15, color='mediumseagreen', edgecolor='black')
                axes[1, 1].set_title('Birth Year Distribution of Top Actors')
                axes[1, 1].set_xlabel('Birth Year')
                axes[1, 1].set_ylabel('Number of Actors')
                axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, f"top_actors_by_rating_{min_films}.png"), dpi=300)
        plt.show()
    
    return actor_stats_with_names


def young_actors_2000s(datasets, save_path=".", years_back=5):
    """
    Analyzes actor age distribution in recent films by age groups.
    Tracks trends in actor demographics and age group demand over time.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        years_back (int): Number of years back from current year. Defaults to 5.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with actor age distribution across five age groups.
    """
    os.makedirs(save_path, exist_ok=True)
    
    from datetime import datetime
    current_year = datetime.now().year
    start_year = current_year - years_back
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Actor age distribution in films {start_year}-{current_year}")
    print("="*80)
    
    basics_df = datasets['title.basics']
    principals_df = datasets['title.principals']
    names_df = datasets['name.basics']
    
    # Filter films from the last N years
    recent_movies = basics_df.filter(
        (F.col("titleType") == "movie") &
        (F.col("startYear") >= start_year) &
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
    
    print(f"\nActor distribution by age groups in films {start_year}-{current_year}:")
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
    
    # Visualization
    age_dist_pd = age_distribution.toPandas()
    yearly_trend_pd = yearly_trend.toPandas()
    
    if not age_dist_pd.empty:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Age group distribution (unique actors)
        axes[0, 0].bar(age_dist_pd['age_group'], age_dist_pd['unique_actors'], color='steelblue')
        axes[0, 0].set_title(f'Unique Actors by Age Group ({start_year}-{current_year})')
        axes[0, 0].set_xlabel('Age Group')
        axes[0, 0].set_ylabel('Number of Unique Actors')
        axes[0, 0].tick_params(axis='x', rotation=45)
        axes[0, 0].grid(True, alpha=0.3, axis='y')
        
        # 2. Total roles by age group
        axes[0, 1].bar(age_dist_pd['age_group'], age_dist_pd['total_roles'], color='coral')
        axes[0, 1].set_title(f'Total Roles by Age Group ({start_year}-{current_year})')
        axes[0, 1].set_xlabel('Age Group')
        axes[0, 1].set_ylabel('Total Number of Roles')
        axes[0, 1].tick_params(axis='x', rotation=45)
        axes[0, 1].grid(True, alpha=0.3, axis='y')
        
        # 3. Average age trend by year
        if not yearly_trend_pd.empty:
            axes[1, 0].plot(yearly_trend_pd['startYear'], yearly_trend_pd['avg_age'], 
                           marker='o', linewidth=2, markersize=8, color='green')
            axes[1, 0].set_title('Average Actor Age Trend Over Years')
            axes[1, 0].set_xlabel('Year')
            axes[1, 0].set_ylabel('Average Age')
            axes[1, 0].grid(True, alpha=0.3)
        
        # 4. Unique actors trend by year
        if not yearly_trend_pd.empty:
            axes[1, 1].bar(yearly_trend_pd['startYear'], yearly_trend_pd['unique_actors'], color='purple')
            axes[1, 1].set_title('Number of Unique Actors by Year')
            axes[1, 1].set_xlabel('Year')
            axes[1, 1].set_ylabel('Unique Actors')
            axes[1, 1].grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, f"young_actors_{start_year}_{current_year}.png"), dpi=300)
        plt.show()
    
    return age_distribution
