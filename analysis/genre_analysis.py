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
    os.makedirs(save_path, exist_ok=True)
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
        if g._legend:
            g._legend.set_bbox_to_anchor((1.05, 0.5))
            g._legend.set_loc("center left")
        g.fig.suptitle(
            f"Genre Popularity Trend (Avg. Votes) - Last {last_n_years} Years", y=1.03
        )
        g.set_axis_labels("Year", "Average Votes")
        plt.savefig(os.path.join(save_path, "genre_votes_trend.png"), bbox_inches='tight')
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
        if g._legend:
            g._legend.set_bbox_to_anchor((1.05, 0.5))
            g._legend.set_loc("center left")
        g.fig.suptitle(
            f"Genre Popularity Trend (Avg. Rating) - Last {last_n_years} Years", y=1.03
        )
        g.set_axis_labels("Year", "Average Rating")
        plt.savefig(os.path.join(save_path, "genre_rating_trend.png"), bbox_inches='tight')
        plt.show()

    return increasing_genres


def genre_actor_cyclicality(
    dataframes, save_path=".", top_n_regions=5, year_range=None, step=10
):
    """
    Analyzes the cyclicality in the popularity of genres and actors in different countries
    by showing the top actor and genre for each time window in each region.

    Args:
        dataframes (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        top_n_regions (int): Number of top regions to analyze.
        year_range (tuple): Optional tuple of (start_year, end_year) to filter data.
                           If None, analyzes all available years.
        step (int): The size of the time window in years (e.g., 10 for decades).
    """
    os.makedirs(save_path, exist_ok=True)

    title_basics = dataframes["title.basics"]
    title_principals = dataframes["title.principals"]
    name_basics = dataframes["name.basics"]
    title_akas = dataframes["title.akas"]

    movies = title_basics.filter(F.col("titleType") == "movie")

    # Determine year range if not provided
    if year_range:
        start_year, end_year = year_range
    else:
        year_stats = movies.agg(F.min("startYear").alias("min_year"), F.max("startYear").alias("max_year")).first()
        if not year_stats or not year_stats["min_year"] or not year_stats["max_year"]:
            print("Could not determine year range from data. Skipping analysis.")
            return
        start_year, end_year = year_stats["min_year"], year_stats["max_year"]
    
    print(f"Analyzing movies from {start_year} to {end_year} in {step}-year steps.")

    movies = movies.filter((F.col("startYear") >= start_year) & (F.col("startYear") <= end_year))

    movie_actors = movies.join(title_principals, "tconst").filter(
        F.col("category").isin(["actor", "actress"])
    )
    movie_actors_names = movie_actors.join(name_basics, "nconst")
    movie_actors_regions = movie_actors_names.join(
        title_akas, movie_actors_names.tconst == title_akas.titleId
    ).filter(F.col("region") != "\\N")

    # Calculate top regions
    region_counts = movie_actors_regions.groupBy("region").count().orderBy(F.desc("count")).limit(top_n_regions)
    top_regions = [row.region for row in region_counts.collect()]
    print(f"Analyzing top {len(top_regions)} regions: {', '.join(top_regions)}")

    # Filter for top regions and create time window column
    regional_data = movie_actors_regions.filter(F.col("region").isin(top_regions)) \
        .withColumn("time_window", (F.floor((F.col("startYear") - start_year) / step) * step) + start_year)

    # --- Actor Analysis ---
    actor_popularity = regional_data.groupBy("time_window", "region", "primaryName").count()
    window_actor = Window.partitionBy("time_window", "region").orderBy(F.desc("count"))
    top_actor_per_window = actor_popularity.withColumn("rank", F.rank().over(window_actor)) \
        .filter(F.col("rank") == 1) \
        .orderBy("time_window", "region")

    top_actors_pd = top_actor_per_window.toPandas()

    if not top_actors_pd.empty:
        fig, ax = plt.subplots(figsize=(18, 10))
        sns.barplot(data=top_actors_pd, x='time_window', y='count', hue='region', ax=ax)

        # --- Robust Annotation Logic ---
        name_lookup = top_actors_pd.set_index(['time_window', 'region'])['primaryName'].to_dict()
        legend = ax.get_legend()
        color_to_hue = {h.get_facecolor(): l.get_text() for h, l in zip(legend.get_patches(), legend.get_texts())}
        x_labels = sorted(top_actors_pd['time_window'].unique())

        for bar in ax.patches:
            if bar.get_height() > 0:
                x_center = bar.get_x() + bar.get_width() / 2
                x_index = int(round(x_center))
                
                if x_index < len(x_labels):
                    x_val = x_labels[x_index]
                    hue_val = color_to_hue.get(bar.get_facecolor())

                    if hue_val:
                        actor_name = name_lookup.get((x_val, hue_val), '')
                        ax.text(x_center, bar.get_height() / 2, actor_name,
                                ha='center', va='center', rotation=90, color='white', fontsize=10)

        ax.set_title(f"Top Actor's Movie Count per {step}-Year Window", fontsize=16)
        ax.set_xlabel(f"{step}-Year Window", fontsize=12)
        ax.set_ylabel("Number of Movies", fontsize=12)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "actor_cyclicality_grouped.png"), bbox_inches='tight')
        plt.show()

    # --- Genre Analysis ---
    genre_popularity = regional_data.withColumn("genre", F.explode(F.split(F.col("genres"), ","))) \
        .groupBy("time_window", "region", "genre").count()
    window_genre = Window.partitionBy("time_window", "region").orderBy(F.desc("count"))
    top_genre_per_window = genre_popularity.withColumn("rank", F.rank().over(window_genre)) \
        .filter(F.col("rank") == 1) \
        .orderBy("time_window", "region")

    top_genres_pd = top_genre_per_window.toPandas()

    if not top_genres_pd.empty:
        fig, ax = plt.subplots(figsize=(18, 10))
        sns.barplot(data=top_genres_pd, x='time_window', y='count', hue='region', ax=ax)

        # --- Robust Annotation Logic ---
        name_lookup = top_genres_pd.set_index(['time_window', 'region'])['genre'].to_dict()
        legend = ax.get_legend()
        color_to_hue = {h.get_facecolor(): l.get_text() for h, l in zip(legend.get_patches(), legend.get_texts())}
        x_labels = sorted(top_genres_pd['time_window'].unique())

        for bar in ax.patches:
            if bar.get_height() > 0:
                x_center = bar.get_x() + bar.get_width() / 2
                x_index = int(round(x_center))

                if x_index < len(x_labels):
                    x_val = x_labels[x_index]
                    hue_val = color_to_hue.get(bar.get_facecolor())

                    if hue_val:
                        genre_name = name_lookup.get((x_val, hue_val), '')
                        ax.text(x_center, bar.get_height() / 2, genre_name,
                                ha='center', va='center', rotation=90, color='white', fontsize=10)

        ax.set_title(f"Top Genre's Movie Count per {step}-Year Window", fontsize=16)
        ax.set_xlabel(f"{step}-Year Window", fontsize=12)
        ax.set_ylabel("Number of Movies", fontsize=12)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "genre_cyclicality_grouped.png"), bbox_inches='tight')
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
    os.makedirs(save_path, exist_ok=True)
    
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
        .filter(F.col("genre") != "\\N")
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
        plt.savefig(os.path.join(save_path, "genre_duration_rating.png"), bbox_inches='tight')
        plt.show()

    return genre_analysis

def genre_evolution_analysis(datasets, save_path=".", split_year=2010, min_films=100):
    """
    Analyzes genre evolution by comparing performance before and after a split year.
    Identifies rising and declining genres based on film volume, rating, and engagement changes.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        split_year (int): Year to split before/after periods. Defaults to 2010.
        min_films (int): Minimum films per period to include genre. Defaults to 100.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with genre evolution metrics including growth percentages and momentum scores.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Genre Evolution Analysis (Before vs After {split_year})")
    print("="*80)
    
    basics_df = datasets['title.basics']
    ratings_df = datasets['title.ratings']
    
    # Define periods
    before_start = split_year - 10
    after_end = split_year + 13  # 2023 approx
    
    print("\nComparing periods:")
    print(f"  BEFORE: {before_start}-{split_year}")
    print(f"  AFTER:  {split_year+1}-{after_end}")
    
    # Filter movies in both periods
    films = basics_df.filter(
        (F.col("titleType") == "movie") & 
        (F.col("startYear") >= before_start) &
        (F.col("startYear") <= after_end) &
        (F.col("genres") != "\\N")
    )
    
    # Join with ratings
    films_with_ratings = films.join(
        ratings_df,
        films.tconst == ratings_df.tconst,
        "inner"
    )
    
    # Mark period
    films_with_ratings = films_with_ratings.withColumn(
        "period",
        F.when(F.col("startYear") <= split_year, "BEFORE").otherwise("AFTER")
    )
    
    # Explode genres
    genres_exploded = films_with_ratings.select(
        F.explode(F.split(F.col("genres"), ",")).alias("genre"),
        "period",
        "averageRating",
        "numVotes"
    )
    
    # Calculate stats per genre per period
    genre_period_stats = genres_exploded.groupBy("genre", "period") \
        .agg(
            F.count("*").alias("film_count"),
            F.avg("averageRating").alias("avg_rating"),
            F.avg("numVotes").alias("avg_votes_per_film")
        )
    
    # Pivot to get before/after columns
    from pyspark.sql.functions import col
    
    before_stats = genre_period_stats.filter(col("period") == "BEFORE") \
        .select(
            col("genre"),
            col("film_count").alias("films_before"),
            col("avg_rating").alias("rating_before"),
            col("avg_votes_per_film").alias("votes_before")
        )
    
    after_stats = genre_period_stats.filter(col("period") == "AFTER") \
        .select(
            col("genre"),
            col("film_count").alias("films_after"),
            col("avg_rating").alias("rating_after"),
            col("avg_votes_per_film").alias("votes_after")
        )
    
    # Join before and after
    genre_evolution = before_stats.join(after_stats, "genre", "inner")
    
    # Filter genres with sufficient films in both periods
    genre_evolution = genre_evolution.filter(
        (col("films_before") >= min_films) & (col("films_after") >= min_films)
    )
    
    # Calculate changes
    genre_evolution = genre_evolution.withColumn(
        "film_growth_pct",
        ((col("films_after") - col("films_before")) / col("films_before") * 100)
    ).withColumn(
        "rating_change",
        col("rating_after") - col("rating_before")
    ).withColumn(
        "engagement_growth_pct",
        ((col("votes_after") - col("votes_before")) / col("votes_before") * 100)
    ).withColumn(
        "momentum_score",
        # Composite: (engagement growth + film growth) / 2 + rating_change * 50
        ((col("engagement_growth_pct") + col("film_growth_pct")) / 2) + (col("rating_change") * 50)
    )
    
    # Show results
    print("\n" + "="*80)
    print("GENRE EVOLUTION RANKINGS")
    print("="*80)
    
    print("\n🚀 TOP 5 RISING GENRES (Highest Momentum):")
    rising = genre_evolution.orderBy(F.desc("momentum_score")).limit(5)
    rising.select(
        "genre",
        F.round("film_growth_pct", 1).alias("film_growth_%"),
        F.round("rating_change", 2).alias("rating_Δ"),
        F.round("engagement_growth_pct", 1).alias("engagement_growth_%"),
        F.round("momentum_score", 1).alias("momentum")
    ).show(truncate=False)
    
    print("\n📉 TOP 5 DECLINING GENRES (Lowest Momentum):")
    declining = genre_evolution.orderBy(F.asc("momentum_score")).limit(5)
    declining.select(
        "genre",
        F.round("film_growth_pct", 1).alias("film_growth_%"),
        F.round("rating_change", 2).alias("rating_Δ"),
        F.round("engagement_growth_pct", 1).alias("engagement_growth_%"),
        F.round("momentum_score", 1).alias("momentum")
    ).show(truncate=False)
    
    # Visualization
    genre_evolution_pd = genre_evolution.toPandas()
    
    if not genre_evolution_pd.empty:
        fig = plt.figure(figsize=(18, 12))
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # 1. Momentum Score (Main metric)
        ax1 = fig.add_subplot(gs[0, :])
        import pandas as pd
        top_bottom = pd.concat([
            genre_evolution_pd.nlargest(7, 'momentum_score'),
            genre_evolution_pd.nsmallest(7, 'momentum_score')
        ]).sort_values('momentum_score')
        colors = ['green' if x > 0 else 'red' for x in top_bottom['momentum_score']]
        ax1.barh(range(len(top_bottom)), top_bottom['momentum_score'], color=colors, alpha=0.7)
        ax1.set_yticks(range(len(top_bottom)))
        ax1.set_yticklabels(top_bottom['genre'], fontsize=9)
        ax1.axvline(0, color='black', linestyle='--', linewidth=1)
        ax1.set_title(f'Genre Momentum Score (Before vs After {split_year})', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Momentum Score')
        ax1.grid(True, alpha=0.3, axis='x')
        
        # 2. Film Volume Growth %
        ax2 = fig.add_subplot(gs[1, 0])
        sorted_by_growth = genre_evolution_pd.sort_values('film_growth_pct', ascending=False).head(10)
        colors2 = ['green' if x > 0 else 'red' for x in sorted_by_growth['film_growth_pct']]
        ax2.barh(range(len(sorted_by_growth)), sorted_by_growth['film_growth_pct'], color=colors2, alpha=0.7)
        ax2.set_yticks(range(len(sorted_by_growth)))
        ax2.set_yticklabels(sorted_by_growth['genre'], fontsize=8)
        ax2.axvline(0, color='black', linestyle='--', linewidth=1)
        ax2.set_title('Film Volume Growth %', fontsize=11, fontweight='bold')
        ax2.set_xlabel('Growth %')
        ax2.grid(True, alpha=0.3, axis='x')
        
        # 3. Rating Change
        ax3 = fig.add_subplot(gs[1, 1])
        sorted_by_rating = genre_evolution_pd.sort_values('rating_change', ascending=False).head(10)
        colors3 = ['green' if x > 0 else 'red' for x in sorted_by_rating['rating_change']]
        ax3.barh(range(len(sorted_by_rating)), sorted_by_rating['rating_change'], color=colors3, alpha=0.7)
        ax3.set_yticks(range(len(sorted_by_rating)))
        ax3.set_yticklabels(sorted_by_rating['genre'], fontsize=8)
        ax3.axvline(0, color='black', linestyle='--', linewidth=1)
        ax3.set_title('Rating Quality Change', fontsize=11, fontweight='bold')
        ax3.set_xlabel('Rating Δ')
        ax3.grid(True, alpha=0.3, axis='x')
        
        # 4. Engagement Growth %
        ax4 = fig.add_subplot(gs[1, 2])
        sorted_by_engagement = genre_evolution_pd.sort_values('engagement_growth_pct', ascending=False).head(10)
        colors4 = ['green' if x > 0 else 'red' for x in sorted_by_engagement['engagement_growth_pct']]
        ax4.barh(range(len(sorted_by_engagement)), sorted_by_engagement['engagement_growth_pct'], color=colors4, alpha=0.7)
        ax4.set_yticks(range(len(sorted_by_engagement)))
        ax4.set_yticklabels(sorted_by_engagement['engagement_growth_pct'], fontsize=8)
        ax4.axvline(0, color='black', linestyle='--', linewidth=1)
        ax4.set_title('Engagement Growth %', fontsize=11, fontweight='bold')
        ax4.set_xlabel('Votes Growth %')
        ax4.grid(True, alpha=0.3, axis='x')
        
        # 5. Scatter: Rating Change vs Engagement Growth
        ax5 = fig.add_subplot(gs[2, 0])
        scatter = ax5.scatter(
            genre_evolution_pd['rating_change'],
            genre_evolution_pd['engagement_growth_pct'],
            s=genre_evolution_pd['films_after'] / 20,
            c=genre_evolution_pd['momentum_score'],
            cmap='RdYlGn',
            alpha=0.6
        )
        ax5.axhline(0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax5.axvline(0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax5.set_xlabel('Rating Change')
        ax5.set_ylabel('Engagement Growth %')
        ax5.set_title('Quality vs Popularity Evolution', fontsize=11, fontweight='bold')
        ax5.grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=ax5, label='Momentum')
        
        # Add genre labels for interesting points
        for idx, row in genre_evolution_pd.iterrows():
            if abs(row['momentum_score']) > genre_evolution_pd['momentum_score'].std() * 1.5:
                ax5.annotate(row['genre'], (row['rating_change'], row['engagement_growth_pct']),
                           fontsize=7, alpha=0.7)
        
        # 6. Before/After Comparison - Film Count
        ax6 = fig.add_subplot(gs[2, 1])
        top_genres_volume = genre_evolution_pd.nlargest(10, 'films_after')
        x = range(len(top_genres_volume))
        width = 0.35
        ax6.barh([i - width/2 for i in x], top_genres_volume['films_before'], 
                width, label=f'Before {split_year}', color='steelblue', alpha=0.7)
        ax6.barh([i + width/2 for i in x], top_genres_volume['films_after'], 
                width, label=f'After {split_year}', color='coral', alpha=0.7)
        ax6.set_yticks(x)
        ax6.set_yticklabels(top_genres_volume['genre'], fontsize=8)
        ax6.set_title('Film Volume: Before vs After', fontsize=11, fontweight='bold')
        ax6.set_xlabel('Number of Films')
        ax6.legend()
        ax6.grid(True, alpha=0.3, axis='x')
        
        # 7. Before/After Comparison - Average Rating
        ax7 = fig.add_subplot(gs[2, 2])
        top_genres_rating = genre_evolution_pd.nlargest(10, 'rating_after')
        x = range(len(top_genres_rating))
        ax7.barh([i - width/2 for i in x], top_genres_rating['rating_before'], 
                width, label=f'Before {split_year}', color='lightgreen', alpha=0.7)
        ax7.barh([i + width/2 for i in x], top_genres_rating['rating_after'], 
                width, label=f'After {split_year}', color='darkgreen', alpha=0.7)
        ax7.set_yticks(x)
        ax7.set_yticklabels(top_genres_rating['genre'], fontsize=8)
        ax7.set_title('Rating: Before vs After', fontsize=11, fontweight='bold')
        ax7.set_xlabel('Average Rating')
        ax7.set_xlim(5, 8)
        ax7.legend()
        ax7.grid(True, alpha=0.3, axis='x')
        
        plt.suptitle(f'Genre Evolution Analysis: {before_start}-{split_year} vs {split_year+1}-{after_end}',
                    fontsize=16, fontweight='bold', y=0.995)
        
        plt.savefig(os.path.join(save_path, "genre_evolution_analysis.png"), dpi=300, bbox_inches='tight')
        plt.show()
    
    return genre_evolution


def genre_combinations_analysis(datasets, save_path=".", min_films=50, top_n=15):
    """
    Analyzes the most popular and successful genre combinations in films.
    Identifies which genre pairings achieve the highest ratings and audience engagement.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        min_films (int): Minimum number of films for a combination to be included. Defaults to 50.
        top_n (int): Number of top combinations to display. Defaults to 15.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with genre combination statistics including ratings and vote counts.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print("BUSINESS QUESTION: Genre Combinations Analysis")
    print("="*80)
    print(f"\nAnalyzing genre combinations with at least {min_films} films")
    
    basics_df = datasets['title.basics']
    ratings_df = datasets['title.ratings']
    
    # Filter for movies with genres
    movies = basics_df.filter(
        (F.col('titleType') == 'movie') & 
        (F.col('genres') != '\\N') &
        (F.col('startYear').isNotNull()) &
        (F.col('startYear').cast('int') >= 1990)  # Focus on modern era
    )
    
    # Join with ratings
    movies_rated = movies.join(ratings_df, 'tconst')
    
    # Filter for combinations (movies with multiple genres)
    # Only keep movies that have more than one genre (contains comma)
    combinations = movies_rated.filter(F.col('genres').contains(','))
    
    # Sort genres alphabetically to treat "Action,Drama" and "Drama,Action" as same
    combinations = combinations.withColumn(
        'genre_combination',
        F.array_sort(F.split(F.col('genres'), ','))
    )
    combinations = combinations.withColumn(
        'genre_combination',
        F.array_join(F.col('genre_combination'), ', ')
    )
    
    # Calculate statistics for each combination
    combo_stats = combinations.groupBy('genre_combination').agg(
        F.count('tconst').alias('film_count'),
        F.avg('averageRating').alias('avg_rating'),
        F.avg('numVotes').alias('avg_votes'),
        F.sum('numVotes').alias('total_votes')
    )
    
    # Filter by minimum films
    combo_stats = combo_stats.filter(F.col('film_count') >= min_films)
    
    # Calculate engagement score (normalized rating * log(votes))
    combo_stats = combo_stats.withColumn(
        'engagement_score',
        (F.col('avg_rating') / 10) * F.log10(F.col('avg_votes') + 1)
    )
    
    # Get top combinations by different metrics
    top_by_rating = combo_stats.orderBy(F.desc('avg_rating')).limit(top_n)
    top_by_popularity = combo_stats.orderBy(F.desc('total_votes')).limit(top_n)
    top_by_engagement = combo_stats.orderBy(F.desc('engagement_score')).limit(top_n)
    
    print("\n" + "-"*80)
    print("TOP GENRE COMBINATIONS BY AVERAGE RATING:")
    print("-"*80)
    top_by_rating.select('genre_combination', 'film_count', 'avg_rating', 'avg_votes').show(top_n, truncate=False)
    
    print("\n" + "-"*80)
    print("TOP GENRE COMBINATIONS BY TOTAL POPULARITY (VOTES):")
    print("-"*80)
    top_by_popularity.select('genre_combination', 'film_count', 'avg_rating', 'total_votes').show(top_n, truncate=False)
    
    # Convert to pandas for visualization
    top_rating_pd = top_by_rating.toPandas()
    top_popularity_pd = top_by_popularity.toPandas()
    top_engagement_pd = top_by_engagement.toPandas()
    
    # Create comprehensive visualization
    plt.figure(figsize=(20, 12))
    
    # 1. Top by Average Rating
    ax1 = plt.subplot(2, 3, 1)
    sns.barplot(data=top_rating_pd.head(10), y='genre_combination', x='avg_rating', 
                palette='viridis', ax=ax1)
    ax1.set_title('Top 10 Genre Combinations by Rating', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Average Rating')
    ax1.set_ylabel('Genre Combination')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # 2. Top by Total Votes (Popularity)
    ax2 = plt.subplot(2, 3, 2)
    top_pop_plot = top_popularity_pd.head(10).copy()
    top_pop_plot['total_votes_millions'] = top_pop_plot['total_votes'] / 1_000_000
    sns.barplot(data=top_pop_plot, y='genre_combination', x='total_votes_millions',
                palette='rocket', ax=ax2)
    ax2.set_title('Top 10 Genre Combinations by Popularity', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Total Votes (Millions)')
    ax2.set_ylabel('Genre Combination')
    ax2.grid(True, alpha=0.3, axis='x')
    
    # 3. Top by Engagement Score
    ax3 = plt.subplot(2, 3, 3)
    sns.barplot(data=top_engagement_pd.head(10), y='genre_combination', x='engagement_score',
                palette='mako', ax=ax3)
    ax3.set_title('Top 10 by Engagement Score', fontsize=12, fontweight='bold')
    ax3.set_xlabel('Engagement Score (Rating × log(Votes))')
    ax3.set_ylabel('Genre Combination')
    ax3.grid(True, alpha=0.3, axis='x')
    
    # 4. Scatter: Rating vs Popularity for all combinations
    ax4 = plt.subplot(2, 3, 4)
    all_combos_pd = combo_stats.toPandas()
    scatter = ax4.scatter(all_combos_pd['avg_rating'], 
                         all_combos_pd['avg_votes'],
                         c=all_combos_pd['film_count'],
                         s=all_combos_pd['film_count']*2,
                         alpha=0.6,
                         cmap='coolwarm')
    ax4.set_xlabel('Average Rating')
    ax4.set_ylabel('Average Votes per Film')
    ax4.set_title('Rating vs Popularity (size = film count)', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=ax4, label='Film Count')
    
    # 5. Film Count Distribution
    ax5 = plt.subplot(2, 3, 5)
    film_counts = top_rating_pd.head(15).copy()
    sns.barplot(data=film_counts, y='genre_combination', x='film_count',
                palette='coolwarm', ax=ax5)
    ax5.set_title('Film Count for Top-Rated Combinations', fontsize=12, fontweight='bold')
    ax5.set_xlabel('Number of Films')
    ax5.set_ylabel('Genre Combination')
    ax5.grid(True, alpha=0.3, axis='x')
    
    # 6. Quality vs Quantity Analysis
    ax6 = plt.subplot(2, 3, 6)
    quality_quant = all_combos_pd.nlargest(20, 'film_count')
    x_pos = range(len(quality_quant))
    ax6_twin = ax6.twinx()
    
    ax6.bar(x_pos, quality_quant['film_count'], alpha=0.6, color='steelblue', label='Film Count')
    ax6_twin.plot(x_pos, quality_quant['avg_rating'], color='red', marker='o', 
                  linewidth=2, label='Avg Rating')
    
    ax6.set_xlabel('Genre Combination (by film count)')
    ax6.set_ylabel('Film Count', color='steelblue')
    ax6_twin.set_ylabel('Average Rating', color='red')
    ax6.set_title('Most Frequent Combinations: Volume vs Quality', fontsize=12, fontweight='bold')
    ax6.set_xticks(x_pos)
    ax6.set_xticklabels(quality_quant['genre_combination'], rotation=45, ha='right', fontsize=8)
    ax6.grid(True, alpha=0.3, axis='y')
    ax6.legend(loc='upper left')
    ax6_twin.legend(loc='upper right')
    
    plt.suptitle('Genre Combinations Analysis: What Works Best Together?', 
                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(os.path.join(save_path, "genre_combinations_analysis.png"), dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n" + "="*80)
    print("KEY INSIGHTS:")
    print("="*80)
    print(f"✓ Analyzed {combo_stats.count()} unique genre combinations")
    print(f"✓ Top-rated combination: {top_rating_pd.iloc[0]['genre_combination']} ({top_rating_pd.iloc[0]['avg_rating']:.2f})")
    print(f"✓ Most popular combination: {top_popularity_pd.iloc[0]['genre_combination']} ({top_popularity_pd.iloc[0]['total_votes']/1e6:.1f}M votes)")
    print("="*80 + "\n")
    
    return combo_stats.orderBy(F.desc('engagement_score'))


def underrated_genre_combos(dataframes, save_path="."):
    """
    Визначає комбінації 2–3 жанрів із високим середнім рейтингом,
    але малою кількістю фільмів (<200).
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    ratings = dataframes["title.ratings"]

    films = basics.filter(
        (F.col("titleType") == "movie") & 
        (F.col("genres").isNotNull())
    ).select("tconst", "genres")

    joined = films.join(ratings, "tconst")

    joined = joined.withColumn("genre_count", F.size(F.split(F.col("genres"), ",")))
    combos = joined.filter((F.col("genre_count") >= 2) & (F.col("genre_count") <= 3))

    combos = combos.withColumn(
        "sorted_genres",
        F.concat_ws(",", F.array_sort(F.split(F.col("genres"), ",")))
    )

    stats = combos.groupBy("sorted_genres").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("film_count")
    )

    rare = stats.filter(F.col("film_count") < 200)

    top_combos = rare.orderBy(F.desc("avg_rating"))

    result_pd = top_combos.limit(20).toPandas()

    if not result_pd.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=result_pd,
            x="sorted_genres", y="avg_rating", palette="viridis"
        )
        plt.title("Топ-20 недооцінених комбінацій жанрів (<200 фільмів)")
        plt.xlabel("Комбінація жанрів")
        plt.ylabel("Середній рейтинг")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "underrated_genre_combos.png"))
        plt.show()

    return top_combos