import os
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from analysis.general import filter_by_region


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


def genre_duration_rating_analysis(dataframes, save_path=".", year_range=None, top_n_regions=5):
    """Analyzes average duration and rating of films by genre and region with clearer visualizations."""
    os.makedirs(save_path, exist_ok=True)
    
    title_basics = dataframes["title.basics"]
    title_ratings = dataframes["title.ratings"]
    title_akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(title_akas, top_n_regions)
    print(f"Analyzing genre duration/rating for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        title_akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

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

    # Join with regional data and ratings
    movies_with_ratings = (
        movies.join(regional_akas, "tconst")
        .join(title_ratings, "tconst")
        .filter(F.col("runtimeMinutes").isNotNull())
    )

    genre_analysis = (
        movies_with_ratings.withColumn(
            "genre", F.explode(F.split(F.col("genres"), ","))
        )
        .filter(F.col("genre") != "\\N")
        .groupBy("region", "genre")
        .agg(
            F.avg("runtimeMinutes").alias("avg_runtime"),
            F.avg("averageRating").alias("avg_rating"),
            F.count("*").alias("movie_count"),
        )
        .filter(F.col("movie_count") > 50)  # Filter for genres with enough data
        .orderBy("region", "genre")
    )

    result_pd = genre_analysis.toPandas()

    if not result_pd.empty:
        # Create two sets of subplots: one for duration, one for rating
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        # Visualization 1: Average Runtime by Genre per Region
        fig1, axes1 = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes1 = axes1.flatten() if num_regions > 1 else [axes1]

        for i, region in enumerate(top_regions):
            ax = axes1[i]
            region_data = result_pd[result_pd["region"] == region].nlargest(15, "movie_count")

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="avg_runtime",
                    y="genre",
                    hue="genre",
                    palette="Blues_r",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Avg Runtime by Genre in {region}{year_suffix}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Runtime (minutes)", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Avg Runtime in {region}")

        for j in range(i + 1, len(axes1)):
            axes1[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"genre_duration_regional{year_suffix.replace(' ', '_')}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        # Visualization 2: Average Rating by Genre per Region
        fig2, axes2 = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes2 = axes2.flatten() if num_regions > 1 else [axes2]

        for i, region in enumerate(top_regions):
            ax = axes2[i]
            region_data = result_pd[result_pd["region"] == region].nlargest(15, "avg_rating")

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="avg_rating",
                    y="genre",
                    hue="genre",
                    palette="Greens_r",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Avg Rating by Genre in {region}{year_suffix}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_ylabel("")
                ax.set_xlim(0, 10)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Avg Rating in {region}")

        for j in range(i + 1, len(axes2)):
            axes2[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"genre_rating_regional{year_suffix.replace(' ', '_')}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return genre_analysis

def genre_evolution_analysis(datasets, save_path=".", split_year=2010, min_films=100, top_n_regions=5):
    """Analyzes genre evolution (before/after split year) by region showing momentum scores."""
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Genre Evolution Analysis by Region (Before vs After {split_year})")
    print("="*80)
    
    basics_df = datasets['title.basics']
    ratings_df = datasets['title.ratings']
    akas_df = datasets['title.akas']

    # Get top regions
    top_regions = get_top_regions(akas_df, top_n_regions)
    print(f"Analyzing genre evolution for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas_df.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )
    
    # Define periods
    before_start = split_year - 10
    after_end = split_year + 13
    
    print(f"\nComparing periods: {before_start}-{split_year} vs {split_year+1}-{after_end}")
    
    # Filter movies in both periods
    films = basics_df.filter(
        (F.col("titleType") == "movie") & 
        (F.col("startYear") >= before_start) &
        (F.col("startYear") <= after_end) &
        (F.col("genres") != "\\N")
    )
    
    # Join with ratings and regional data
    films_with_ratings = films.join(ratings_df, "tconst").join(regional_akas, "tconst")
    
    # Mark period
    films_with_ratings = films_with_ratings.withColumn(
        "period",
        F.when(F.col("startYear") <= split_year, "BEFORE").otherwise("AFTER")
    )
    
    # Explode genres
    genres_exploded = films_with_ratings.select(
        "region",
        F.explode(F.split(F.col("genres"), ",")).alias("genre"),
        "period",
        "averageRating",
        "numVotes"
    )
    
    # Calculate stats per genre per period per region
    genre_period_stats = genres_exploded.groupBy("region", "genre", "period").agg(
        F.count("*").alias("film_count"),
        F.avg("averageRating").alias("avg_rating"),
        F.avg("numVotes").alias("avg_votes_per_film")
    )
    
    # Pivot to get before/after columns
    from pyspark.sql.functions import col
    
    before_stats = genre_period_stats.filter(col("period") == "BEFORE").select(
        col("region"),
        col("genre"),
        col("film_count").alias("films_before"),
        col("avg_rating").alias("rating_before"),
        col("avg_votes_per_film").alias("votes_before")
    )
    
    after_stats = genre_period_stats.filter(col("period") == "AFTER").select(
        col("region"),
        col("genre"),
        col("film_count").alias("films_after"),
        col("avg_rating").alias("rating_after"),
        col("avg_votes_per_film").alias("votes_after")
    )
    
    # Join before and after
    genre_evolution = before_stats.join(after_stats, ["region", "genre"], "inner")
    
    # Filter genres with sufficient films in both periods
    genre_evolution = genre_evolution.filter(
        (col("films_before") >= min_films) & (col("films_after") >= min_films)
    )
    
    # Calculate momentum score
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
        ((col("engagement_growth_pct") + col("film_growth_pct")) / 2) + (col("rating_change") * 50)
    )
    
    # Get top genres by momentum per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("momentum_score"))
    top_genres = (
        genre_evolution.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= 10)
        .orderBy("region", "rank")
    )

    result_pd = top_genres.toPandas()

    if not result_pd.empty:
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
                # Color based on positive/negative momentum
                colors = ['green' if x > 0 else 'red' for x in region_data['momentum_score']]
                
                ax.barh(range(len(region_data)), region_data['momentum_score'], color=colors, alpha=0.7)
                ax.set_yticks(range(len(region_data)))
                ax.set_yticklabels(region_data['genre'], fontsize=9)
                ax.axvline(0, color='black', linestyle='--', linewidth=1)
                ax.set_title(
                    f"Genre Momentum in {region}\n({before_start}-{split_year} vs {split_year+1}-{after_end})",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Momentum Score", fontsize=10)
                ax.grid(True, alpha=0.3, axis='x')
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Genre Evolution in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "genre_evolution_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_genres


def genre_combinations_analysis(datasets, save_path=".", min_films=50, top_n=15, top_n_regions=5):
    """Analyzes successful genre combinations by region."""
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print("BUSINESS QUESTION: Genre Combinations Analysis by Region")
    print("="*80)
    
    basics_df = datasets['title.basics']
    ratings_df = datasets['title.ratings']
    akas_df = datasets['title.akas']

    # Get top regions
    top_regions = get_top_regions(akas_df, top_n_regions)
    print(f"Analyzing genre combinations for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas_df.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )
    
    # Filter for movies with genres
    movies = basics_df.filter(
        (F.col('titleType') == 'movie') & 
        (F.col('genres') != '\\N') &
        (F.col('startYear').isNotNull()) &
        (F.col('startYear').cast('int') >= 1990)
    )
    
    # Join with ratings and regional data
    movies_rated = movies.join(ratings_df, 'tconst').join(regional_akas, 'tconst')
    
    # Filter for combinations (movies with multiple genres)
    combinations = movies_rated.filter(F.col('genres').contains(','))
    
    # Sort genres alphabetically
    combinations = combinations.withColumn(
        'genre_combination',
        F.array_sort(F.split(F.col('genres'), ','))
    ).withColumn(
        'genre_combination',
        F.array_join(F.col('genre_combination'), ', ')
    )
    
    # Calculate statistics per region
    combo_stats = combinations.groupBy('region', 'genre_combination').agg(
        F.count('tconst').alias('film_count'),
        F.avg('averageRating').alias('avg_rating'),
        F.avg('numVotes').alias('avg_votes')
    ).filter(F.col('film_count') >= min_films)
    
    # Get top combinations per region by rating
    w_rank = Window.partitionBy("region").orderBy(F.desc("avg_rating"))
    top_combos = (
        combo_stats.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= top_n)
        .orderBy("region", "rank")
    )

    result_pd = top_combos.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(top_n)

            if not region_data.empty:
                # Truncate long combination names
                region_data = region_data.copy()
                region_data['combo_short'] = region_data['genre_combination'].str[:40]
                
                sns.barplot(
                    data=region_data,
                    x="avg_rating",
                    y="combo_short",
                    hue="combo_short",
                    palette="viridis",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Top Genre Combos in {region}\n(by Rating, min {min_films} films)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_ylabel("")
                ax.set_xlim(0, 10)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Genre Combos in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "genre_combinations_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_combos


def underrated_genre_combos(dataframes, save_path=".", top_n_regions=5):
    """Identifies underrated genre combinations (high rating, low production) by region."""
    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing underrated genre combos for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    films = basics.filter(
        (F.col("titleType") == "movie") & 
        (F.col("genres").isNotNull())
    ).select("tconst", "genres")

    joined = films.join(ratings, "tconst").join(regional_akas, "tconst")

    joined = joined.withColumn("genre_count", F.size(F.split(F.col("genres"), ",")))
    combos = joined.filter((F.col("genre_count") >= 2) & (F.col("genre_count") <= 3))

    combos = combos.withColumn(
        "sorted_genres",
        F.concat_ws(",", F.array_sort(F.split(F.col("genres"), ",")))
    )

    stats = combos.groupBy("region", "sorted_genres").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("film_count")
    )

    rare = stats.filter(F.col("film_count") < 200)

    # Get top underrated combos per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("avg_rating"))
    top_combos = (
        rare.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= 15)
        .orderBy("region", "rank")
    )

    result_pd = top_combos.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(15)

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="avg_rating",
                    y="sorted_genres",
                    hue="sorted_genres",
                    palette="viridis",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Underrated Genre Combos in {region}\n(<200 films)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_ylabel("")
                ax.set_xlim(0, 10)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Underrated Combos in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "underrated_genre_combos_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_combos