import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

def correlation_seasons_rating(dataframes, save_path="."):    
    os.makedirs(save_path, exist_ok=True)
    
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"].filter(F.col("titleType") == "tvSeries")
    
    # Get season counts
    seasons_count = episodes.groupBy("parentTconst").agg(
        F.max("seasonNumber").alias("num_seasons")
    )
    
    tv_with_seasons = basics.join(
        seasons_count, 
        basics.tconst == seasons_count.parentTconst
    ).select(basics.tconst, "num_seasons")
    
    tv_with_ratings = tv_with_seasons.join(ratings, "tconst")
    
    # Create season groups: 1-19 individual, 20+ grouped
    tv_with_ratings = tv_with_ratings.withColumn(
        "season_group",
        F.when(F.col("num_seasons") >= 20, "20+")
         .otherwise(F.col("num_seasons").cast("string"))
    )
    
    # Aggregate by season groups
    corr_data = tv_with_ratings.groupBy("season_group").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("count")
    )
    
    corr_data_pd = corr_data.toPandas()
    
    if not corr_data_pd.empty:
        # Convert season_group to numeric for sorting (20+ goes at end)
        def season_sort_key(x):
            if x == "20+":
                return 999
            return int(x)
        
        corr_data_pd['sort_key'] = corr_data_pd['season_group'].apply(season_sort_key)
        corr_data_pd = corr_data_pd.sort_values('sort_key')
        
        # Create bar plot
        fig, ax1 = plt.subplots(figsize=(16, 8))
        
        # Color bars by rating (gradient)
        colors = plt.cm.RdYlGn((corr_data_pd['avg_rating'] - corr_data_pd['avg_rating'].min()) / 
                                (corr_data_pd['avg_rating'].max() - corr_data_pd['avg_rating'].min()))
        
        # Primary bars: Average rating
        bars = ax1.bar(
            range(len(corr_data_pd)),
            corr_data_pd['avg_rating'],
            color=colors,
            edgecolor='black',
            linewidth=1.5,
            alpha=0.8
        )
        
        ax1.set_xlabel("Number of Seasons", fontsize=13, fontweight='bold')
        ax1.set_ylabel("Average Rating", fontsize=13, fontweight='bold', color='black')
        ax1.set_title("TV Series: Average Rating by Number of Seasons", 
                     fontsize=15, fontweight='bold', pad=20)
        ax1.set_xticks(range(len(corr_data_pd)))
        ax1.set_xticklabels(corr_data_pd['season_group'], rotation=45, ha='right')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(True, alpha=0.3, axis='y', linestyle='--')
        ax1.set_ylim(0, 10)
        
        # Add rating values on top of bars
        for i, (idx, row) in enumerate(corr_data_pd.iterrows()):
            ax1.text(
                i,
                row['avg_rating'] + 0.15,
                f"{row['avg_rating']:.2f}",
                ha='center',
                va='bottom',
                fontsize=9,
                fontweight='bold'
            )
        
        # Secondary axis for count
        ax2 = ax1.twinx()
        ax2.plot(
            range(len(corr_data_pd)),
            corr_data_pd['count'],
            color='navy',
            marker='o',
            linewidth=2.5,
            markersize=8,
            label='Number of Series',
            alpha=0.7
        )
        ax2.set_ylabel("Number of TV Series", fontsize=13, fontweight='bold', color='navy')
        ax2.tick_params(axis='y', labelcolor='navy')
        ax2.legend(loc='upper right', fontsize=11)
        
        # Add count labels
        for i, (idx, row) in enumerate(corr_data_pd.iterrows()):
            ax2.text(
                i,
                row['count'] + max(corr_data_pd['count']) * 0.02,
                f"{row['count']:,}",
                ha='center',
                va='bottom',
                fontsize=8,
                color='navy',
                alpha=0.7
            )
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "seasons_rating_correlation.png"), 
                   dpi=300, bbox_inches='tight')
        plt.show()
        
        # Print correlation coefficient
        # Use original numeric seasons for correlation (exclude 20+)
        tv_numeric = tv_with_ratings.filter(F.col("num_seasons") < 20)
        correlation = tv_numeric.stat.corr("num_seasons", "averageRating")
        print(f"Correlation coefficient (seasons 1-19): {correlation:.4f}")
        
    return corr_data


def top_episodes_by_votes_and_rating(dataframes, save_path="."):
    os.makedirs(save_path, exist_ok=True)
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"]

    ep_with_ratings = episodes.alias("ep").join(ratings.alias("r"), F.col("ep.tconst") == F.col("r.tconst")) \
                              .join(basics.alias("b_ep"), F.col("ep.tconst") == F.col("b_ep.tconst")) \
                              .join(basics.alias("b_parent"), F.col("ep.parentTconst") == F.col("b_parent.tconst")) \
                              .select(
                                  F.col("ep.tconst"),
                                  F.col("b_ep.primaryTitle").alias("episodeTitle"),
                                  F.col("b_parent.primaryTitle").alias("seriesTitle"),
                                  F.col("ep.seasonNumber"),
                                  F.col("ep.episodeNumber"),
                                  F.col("r.numVotes"),
                                  F.col("r.averageRating")
                              )

    top_episodes = ep_with_ratings.orderBy(F.desc("numVotes")).select(
        "tconst",
        "episodeTitle",
        "seriesTitle",
        "seasonNumber",
        "episodeNumber",
        "numVotes",
        "averageRating"
    ).limit(20)

    top_episodes_pd = top_episodes.toPandas()

    if not top_episodes_pd.empty:
        plt.figure(figsize=(12, 10))
        top_episodes_pd['fullTitle'] = top_episodes_pd.apply(
            lambda row: f"{row['seriesTitle']} - S{row['seasonNumber']}E{row['episodeNumber']}: {row['episodeTitle']}",
            axis=1
        )
        sns.barplot(data=top_episodes_pd, x="numVotes", y="fullTitle", orient='h')
        plt.title("Top 20 TV Episodes by Number of Votes")
        plt.xlabel("Number of Votes")
        plt.ylabel("Episode (Series - SeasonEpisode: Title)")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "top_episodes_by_votes.png"))
        plt.show()

    return top_episodes


def genre_seasons_influence(dataframes, save_path="."):
    """
    Як жанрове різноманіття впливає на кількість сезонів серіалів
    """
    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]

    series = basics.filter(F.col("titleType") == "tvSeries").select("tconst", "genres")
    seasons = episodes.groupBy("parentTconst").agg(F.max("seasonNumber").alias("num_seasons"))
    series_with_genres = series.join(seasons, series.tconst == seasons.parentTconst)
    exploded = series_with_genres.withColumn("genre", F.explode(F.split("genres", ","))).select("genre", "num_seasons")
    
    stats = exploded.groupBy("genre").agg(
        F.avg("num_seasons").alias("avg_seasons"),
        F.max("num_seasons").alias("max_seasons"),
        F.min("num_seasons").alias("min_seasons"),
        F.count("genre").alias("num_series")
    ).orderBy(F.desc("avg_seasons"))

    stats_pd = stats.filter(F.col("num_series") > 100).toPandas() # Filter for genres with a decent number of series

    if not stats_pd.empty:
        plt.figure(figsize=(12, 8))
        sns.barplot(data=stats_pd, x="avg_seasons", y="genre", orient='h')
        plt.title("Influence of Genre on Average Number of Seasons")
        plt.xlabel("Average Number of Seasons")
        plt.ylabel("Genre")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "genre_seasons_influence.png"))
        plt.show()

    return stats


def avg_rating_long_series(datasets, save_path=".", min_episodes=50):
    """
    Analyzes the average rating of TV series with more than N episodes.
    Provides insights into audience preferences for long-running series.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        min_episodes (int): Minimum number of episodes. Defaults to 50.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with average rating statistics for long series.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Average rating of series with {min_episodes}+ episodes")
    print("="*80)
    
    basics_df = datasets['title.basics']
    episodes_df = datasets['title.episode']
    ratings_df = datasets['title.ratings']
    
    # Count episodes for each series
    episode_counts = episodes_df.groupBy("parentTconst") \
        .agg(F.count("*").alias("episode_count"))
    
    # Filter series with more than min_episodes
    long_series = episode_counts.filter(F.col("episode_count") > min_episodes)
    
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
    
    print(f"\nSeries with {min_episodes}+ episodes:")
    print(f"Series count: {avg_rating.first()['series_count']}")
    print(f"Average rating: {avg_rating.first()['avg_rating']:.2f}")
    
    # Show top-10 long series with highest ratings
    print("\nTop-10 long series with highest ratings:")
    top_series = series_with_ratings.select(
        "primaryTitle", "episode_count", "averageRating", "numVotes"
    ).orderBy(F.desc("averageRating")).limit(10)
    top_series.show(truncate=False)
    
    # Visualization
    series_pd = series_with_ratings.select(
        "primaryTitle", "episode_count", "averageRating", "numVotes"
    ).orderBy(F.desc("numVotes")).limit(20).toPandas()
    
    if not series_pd.empty:
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        # 1. Rating distribution
        axes[0].hist(series_pd['averageRating'], bins=20, color='skyblue', edgecolor='black')
        axes[0].axvline(avg_rating.first()['avg_rating'], color='red', linestyle='--', 
                       label=f'Average: {avg_rating.first()["avg_rating"]:.2f}')
        axes[0].set_title(f'Rating Distribution of Series with {min_episodes}+ Episodes')
        axes[0].set_xlabel('Average Rating')
        axes[0].set_ylabel('Number of Series')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        
        # 2. Top 20 series by votes
        top_20 = series_pd.nsmallest(20, 'numVotes', keep='last')
        axes[1].barh(range(len(top_20)), top_20['averageRating'])
        axes[1].set_yticks(range(len(top_20)))
        axes[1].set_yticklabels(top_20['primaryTitle'], fontsize=8)
        axes[1].set_title('Top 20 Most Popular Long Series (by votes)')
        axes[1].set_xlabel('Average Rating')
        axes[1].grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, f"long_series_ratings_{min_episodes}.png"), dpi=300)
        plt.show()
    
    return avg_rating


def season_rating_diff(datasets, save_path=".", min_seasons=3):
    """
    Analyzes rating differences between consecutive seasons for TV series.
    Uses window functions to track season-to-season rating dynamics.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        min_seasons (int): Minimum number of seasons. Defaults to 3.
    
    Returns:
        pyspark.sql.DataFrame: A DataFrame with season-to-season rating differences and trends.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*80)
    print(f"BUSINESS QUESTION: Season rating dynamics for series with {min_seasons}+ seasons")
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
    
    # Filter series with more than min_seasons
    long_series = season_counts.filter(F.col("max_season") > min_seasons)
    
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
        .select("primaryTitle", "seasonNumber", 
                F.round("season_avg_rating", 2).alias("season_avg_rating"), 
                F.round("prev_season_rating", 2).alias("prev_season_rating"), 
                F.round("rating_diff", 2).alias("rating_diff")) \
        .orderBy(F.desc("rating_diff")) \
        .limit(10) \
        .show(truncate=False)
    
    print("\nSeries with biggest rating drop between seasons:")
    result.filter(F.col("rating_diff").isNotNull()) \
        .select("primaryTitle", "seasonNumber", 
                F.round("season_avg_rating", 2).alias("season_avg_rating"), 
                F.round("prev_season_rating", 2).alias("prev_season_rating"), 
                F.round("rating_diff", 2).alias("rating_diff")) \
        .orderBy(F.asc("rating_diff")) \
        .limit(10) \
        .show(truncate=False)
    
    # Visualization
    result_pd = result.filter(F.col("rating_diff").isNotNull()).toPandas()
    
    if not result_pd.empty:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Distribution of rating differences
        axes[0, 0].hist(result_pd['rating_diff'], bins=50, color='lightcoral', edgecolor='black')
        axes[0, 0].axvline(0, color='red', linestyle='--', linewidth=2, label='No change')
        axes[0, 0].set_title('Distribution of Season-to-Season Rating Changes')
        axes[0, 0].set_xlabel('Rating Difference')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # 2. Top 10 improvements
        top_improvements = result_pd.nlargest(10, 'rating_diff')
        axes[0, 1].barh(range(len(top_improvements)), top_improvements['rating_diff'], color='green')
        axes[0, 1].set_yticks(range(len(top_improvements)))
        axes[0, 1].set_yticklabels([f"{row['primaryTitle'][:30]} (S{int(row['seasonNumber'])})" 
                                    for _, row in top_improvements.iterrows()], fontsize=8)
        axes[0, 1].set_title('Top 10 Season Rating Improvements')
        axes[0, 1].set_xlabel('Rating Increase')
        axes[0, 1].grid(True, alpha=0.3, axis='x')
        
        # 3. Top 10 declines
        top_declines = result_pd.nsmallest(10, 'rating_diff')
        axes[1, 0].barh(range(len(top_declines)), top_declines['rating_diff'], color='red')
        axes[1, 0].set_yticks(range(len(top_declines)))
        axes[1, 0].set_yticklabels([f"{row['primaryTitle'][:30]} (S{int(row['seasonNumber'])})" 
                                   for _, row in top_declines.iterrows()], fontsize=8)
        axes[1, 0].set_title('Top 10 Season Rating Declines')
        axes[1, 0].set_xlabel('Rating Decrease')
        axes[1, 0].grid(True, alpha=0.3, axis='x')
        
        # 4. Average rating by season number
        season_avg = result_pd.groupby('seasonNumber')['season_avg_rating'].mean().reset_index()
        axes[1, 1].plot(season_avg['seasonNumber'], season_avg['season_avg_rating'], 
                       marker='o', linewidth=2, markersize=8)
        axes[1, 1].set_title('Average Rating by Season Number')
        axes[1, 1].set_xlabel('Season Number')
        axes[1, 1].set_ylabel('Average Rating')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, f"season_rating_trends_{min_seasons}.png"), dpi=300)
        plt.show()
    
    return result


def hook_shows(dataframes, save_path="."):
    """
    Визначення серіалів, у яких рейтинг фіналу сезону 1 > рейтингу пілота.
    (Серіали, що "зачепили" глядача )
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]

    series = basics.filter(F.col("titleType") == "tvSeries").select("tconst", "primaryTitle")

    season1_eps = episodes.filter(F.col("seasonNumber") == 1).select("tconst", "parentTconst", "episodeNumber")

    eps_with_ratings = season1_eps.join(ratings, "tconst")

    agg = eps_with_ratings.groupBy("parentTconst").agg(
        F.first("averageRating").alias("first_rating"),
        F.last("averageRating").alias("last_rating"),
        F.min("episodeNumber").alias("first_ep"),
        F.max("episodeNumber").alias("last_ep"),
    )

    agg = agg.withColumn("delta_rating", F.col("last_rating") - F.col("first_rating"))

    growing = agg.filter(F.col("delta_rating") > 0)

    result = growing.join(series, series["tconst"] == growing["parentTconst"]) \
                    .select("primaryTitle", "first_rating", "last_rating", "delta_rating") \
                    .orderBy(F.desc("delta_rating"))

    result_pd = result.limit(20).toPandas()  

    if not result_pd.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(data=result_pd, x="primaryTitle", y="delta_rating")
        plt.title("Top 20 'Hook' TV Series — Growth in Rating (Season 1)")
        plt.xlabel("TV Series")
        plt.ylabel("Rating Growth (Final - Pilot)")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "hook_shows.png"))
        plt.show()

    return result


def sophomore_slump(dataframes, save_path="."):
    """
    Аналіз "синдрому другого сезону":
    середня зміна рейтингу між фіналом 1-го та 2-го сезонів для серіалів із 3+ сезонами.
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]

    eps = episodes.filter(
        F.col("seasonNumber").isNotNull() &
        F.col("episodeNumber").isNotNull()
    ).select("tconst", "parentTconst", "seasonNumber", "episodeNumber")

    eps = eps.join(ratings.select("tconst", "averageRating"), "tconst")

    w = Window.partitionBy("parentTconst", "seasonNumber")
    season_finals = eps.withColumn("max_ep", F.max("episodeNumber").over(w)) \
                       .filter(F.col("episodeNumber") == F.col("max_ep")) \
                       .select("parentTconst", "seasonNumber", "averageRating")

    valid_series = season_finals.groupBy("parentTconst") \
                                .agg(F.countDistinct("seasonNumber").alias("num_seasons")) \
                                .filter(F.col("num_seasons") >= 3)

    season_finals = season_finals.join(valid_series, "parentTconst")

    season_subset = season_finals.filter(F.col("seasonNumber").isin([1, 2]))

    pivoted = season_subset.groupBy("parentTconst").pivot("seasonNumber").agg(F.first("averageRating")) \
                           .withColumnRenamed("1", "rating_s1_final") \
                           .withColumnRenamed("2", "rating_s2_final")

    diff = pivoted.withColumn("delta", F.col("rating_s2_final") - F.col("rating_s1_final"))

    result = diff.join(
        basics.select("tconst", "primaryTitle", "genres"),
        diff["parentTconst"] == basics["tconst"],
        "left"
    ).select("primaryTitle", "genres", "rating_s1_final", "rating_s2_final", "delta")

    genre_stats = result.withColumn("main_genre", F.split(F.col("genres"), ",")[0]) \
                        .groupBy("main_genre") \
                        .agg(F.avg("delta").alias("avg_delta"),
                             F.count("primaryTitle").alias("num_series")) \
                        .filter(F.col("num_series") >= 5) \
                        .orderBy("avg_delta")

    genre_pd = genre_stats.toPandas()

    if not genre_pd.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(data=genre_pd, x="main_genre", y="avg_delta", palette="coolwarm")
        plt.title("'Синдром другого сезону' — середня зміна рейтингу між 1 і 2 сезоном")
        plt.xlabel("Жанр")
        plt.ylabel("Δ рейтинг (2 сезон - 1 сезон)")
        plt.axhline(0, color='gray', linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "sophomore_slump.png"))
        plt.show()

    return result, genre_stats