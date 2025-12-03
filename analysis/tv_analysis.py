import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql import Window
from analysis.general import filter_by_region, get_top_regions


def correlation_seasons_rating(dataframes, save_path=".", top_n_regions=5):
    """Analyze correlation between number of seasons and rating for scripted TV series by region."""
    os.makedirs(save_path, exist_ok=True)

    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"].filter(F.col("titleType") == "tvSeries")
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing seasons vs rating for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter out news, talk shows, game shows, reality TV
    # Keep only scripted series
    non_scripted_genres = ["News", "Talk-Show", "Game-Show", "Reality-TV"]
    basics_scripted = basics.filter(
        ~F.array_contains(F.split(F.col("genres"), ","), "News")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Talk-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Game-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Reality-TV")
    )

    # Get season counts
    seasons_count = episodes.groupBy("parentTconst").agg(
        F.max("seasonNumber").alias("num_seasons")
    )

    # Join with regional data
    tv_with_seasons = (
        basics_scripted.join(regional_akas, "tconst")
        .join(seasons_count, basics_scripted.tconst == seasons_count.parentTconst)
        .select("region", basics_scripted.tconst, "num_seasons")
    )

    tv_with_ratings = tv_with_seasons.join(ratings, "tconst")

    # Create season groups: 1-19 individual, 20+ grouped
    tv_with_ratings = tv_with_ratings.withColumn(
        "season_group",
        F.when(F.col("num_seasons") >= 20, "20+").otherwise(
            F.col("num_seasons").cast("string")
        ),
    )

    # Aggregate by region and season groups
    corr_data = tv_with_ratings.groupBy("region", "season_group").agg(
        F.avg("averageRating").alias("avg_rating"), F.count("tconst").alias("count")
    )

    result_pd = corr_data.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].copy()

            if not region_data.empty:
                # Filter out None values
                region_data = region_data[region_data["season_group"].notna()]

                def season_sort_key(x):
                    if x == "20+":
                        return 999
                    return int(x)

                region_data["sort_key"] = region_data["season_group"].apply(
                    season_sort_key
                )
                region_data = region_data.sort_values("sort_key")

                # Color bars by rating
                if len(region_data) > 0:
                    colors = plt.cm.RdYlGn(
                        (region_data["avg_rating"] - region_data["avg_rating"].min())
                        / (
                            region_data["avg_rating"].max()
                            - region_data["avg_rating"].min()
                            + 0.001
                        )
                    )

                    ax.bar(
                        range(len(region_data)),
                        region_data["avg_rating"],
                        color=colors,
                        edgecolor="black",
                        linewidth=1,
                        alpha=0.8,
                    )

                    ax.set_title(
                        f"Seasons vs Rating in {region}\n(Scripted Series Only)",
                        fontsize=12,
                        fontweight="bold",
                    )
                    ax.set_xlabel("Number of Seasons", fontsize=10)
                    ax.set_ylabel("Average Rating", fontsize=10)
                    ax.set_xticks(range(len(region_data)))
                    ax.set_xticklabels(
                        region_data["season_group"], rotation=45, ha="right"
                    )
                    ax.set_ylim(0, 10)
                    ax.grid(True, alpha=0.3, axis="y")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Seasons vs Rating in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "seasons_rating_correlation_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return corr_data


def top_episodes_by_votes_and_rating(dataframes, save_path=".", top_n_regions=5):
    """Find top episodes by votes for scripted series by region."""
    os.makedirs(save_path, exist_ok=True)
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"]
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing top episodes for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter out non-scripted shows from parent series
    basics_scripted = basics.filter(
        (F.col("titleType") == "tvSeries")
        & ~F.array_contains(F.split(F.col("genres"), ","), "News")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Talk-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Game-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Reality-TV")
    )

    # Join episodes with regional data and parent series info
    ep_with_ratings = (
        episodes.alias("ep")
        .join(ratings.alias("r"), F.col("ep.tconst") == F.col("r.tconst"))
        .join(
            basics_scripted.alias("b_parent"),
            F.col("ep.parentTconst") == F.col("b_parent.tconst"),
        )
        .join(
            regional_akas.alias("ra"),
            F.col("b_parent.tconst") == F.col("ra.tconst"),
        )
        .join(basics.alias("b_ep"), F.col("ep.tconst") == F.col("b_ep.tconst"), "left")
        .select(
            F.col("ra.region"),
            F.col("ep.tconst"),
            F.coalesce(F.col("b_ep.primaryTitle"), F.lit("Unknown")).alias(
                "episodeTitle"
            ),
            F.col("b_parent.primaryTitle").alias("seriesTitle"),
            F.col("ep.seasonNumber"),
            F.col("ep.episodeNumber"),
            F.col("r.numVotes"),
            F.col("r.averageRating"),
        )
    )

    # Get top 20 per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("numVotes"))
    top_episodes = (
        ep_with_ratings.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= 20)
        .orderBy("region", "rank")
    )

    result_pd = top_episodes.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(20)

            if not region_data.empty:
                region_data["fullTitle"] = region_data.apply(
                    lambda row: f"{row['seriesTitle'][:30]} S{int(row['seasonNumber'])}E{int(row['episodeNumber'])}",
                    axis=1,
                )
                sns.barplot(
                    data=region_data,
                    x="numVotes",
                    y="fullTitle",
                    hue="fullTitle",
                    palette="viridis",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Top 20 Episodes in {region}\n(Scripted Series Only)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Number of Votes", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Top Episodes in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "top_episodes_by_votes_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_episodes


def genre_seasons_influence(dataframes, save_path=".", top_n_regions=5):
    """Analyze how genre influences number of seasons for scripted series by region."""
    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing genre influence on seasons for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter out non-scripted shows
    series = (
        basics.filter(F.col("titleType") == "tvSeries")
        .filter(
            ~F.array_contains(F.split(F.col("genres"), ","), "News")
            & ~F.array_contains(F.split(F.col("genres"), ","), "Talk-Show")
            & ~F.array_contains(F.split(F.col("genres"), ","), "Game-Show")
            & ~F.array_contains(F.split(F.col("genres"), ","), "Reality-TV")
        )
        .select("tconst", "genres")
    )

    seasons = episodes.groupBy("parentTconst").agg(
        F.max("seasonNumber").alias("num_seasons")
    )

    # Join with regional data
    series_with_genres = (
        series.join(regional_akas, "tconst")
        .join(seasons, series.tconst == seasons.parentTconst)
        .select("region", "genres", "num_seasons")
    )

    exploded = series_with_genres.withColumn(
        "genre", F.explode(F.split("genres", ","))
    ).select("region", "genre", "num_seasons")

    stats = (
        exploded.groupBy("region", "genre")
        .agg(
            F.avg("num_seasons").alias("avg_seasons"),
            F.count("genre").alias("num_series"),
        )
        .filter(F.col("num_series") > 20)
        .orderBy("region", F.desc("avg_seasons"))
    )

    result_pd = stats.toPandas()

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
                    x="avg_seasons",
                    y="genre",
                    hue="genre",
                    palette="coolwarm",
                    legend=False,
                    orient="h",
                    ax=ax,
                )
                ax.set_title(
                    f"Genre Influence on Seasons in {region}\n(Scripted Series Only)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Avg Number of Seasons", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Genre Influence in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "genre_seasons_influence_regional.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return stats


def avg_rating_long_series(datasets, save_path=".", min_episodes=50, top_n_regions=5):
    """Analyzes average rating of long-running scripted TV series by region."""
    os.makedirs(save_path, exist_ok=True)

    print("\n" + "=" * 80)
    print(
        f"BUSINESS QUESTION: Average rating of scripted series with {min_episodes}+ episodes by region"
    )
    print("=" * 80)

    basics_df = datasets["title.basics"]
    episodes_df = datasets["title.episode"]
    ratings_df = datasets["title.ratings"]
    akas_df = datasets["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas_df, top_n_regions)
    print(f"Analyzing long series for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas_df.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter out non-scripted shows
    basics_scripted = basics_df.filter(
        (F.col("titleType") == "tvSeries")
        & ~F.array_contains(F.split(F.col("genres"), ","), "News")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Talk-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Game-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Reality-TV")
    )

    # Count episodes for each series
    episode_counts = episodes_df.groupBy("parentTconst").agg(
        F.count("*").alias("episode_count")
    )

    # Filter series with more than min_episodes
    long_series = episode_counts.filter(F.col("episode_count") > min_episodes)

    # Join with regional data and basics
    series_info = (
        long_series.join(
            basics_scripted, long_series.parentTconst == basics_scripted.tconst
        )
        .join(regional_akas, "tconst")
        .join(ratings_df, "tconst")
        .select("region", "primaryTitle", "episode_count", "averageRating", "numVotes")
    )

    result_pd = series_info.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].nlargest(
                20, "numVotes"
            )

            if not region_data.empty:
                ax.barh(
                    range(len(region_data)),
                    region_data["averageRating"],
                    color="skyblue",
                )
                ax.set_yticks(range(len(region_data)))
                ax.set_yticklabels(region_data["primaryTitle"].str[:30], fontsize=8)
                ax.set_title(
                    f"Top Long Series in {region}\n({min_episodes}+ episodes, Scripted Only)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_xlim(0, 10)
                ax.grid(True, alpha=0.3, axis="x")
                ax.invert_yaxis()
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Long Series in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"long_series_ratings_regional_{min_episodes}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return series_info


def season_rating_diff(datasets, save_path=".", min_seasons=3, top_n_regions=5):
    """Analyzes season-to-season rating changes for scripted TV series by region."""
    os.makedirs(save_path, exist_ok=True)

    print("\n" + "=" * 80)
    print(
        f"BUSINESS QUESTION: Season rating dynamics for scripted series with {min_seasons}+ seasons by region"
    )
    print("=" * 80)

    episodes_df = datasets["title.episode"]
    ratings_df = datasets["title.ratings"]
    basics_df = datasets["title.basics"]
    akas_df = datasets["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas_df, top_n_regions)
    print(f"Analyzing season rating changes for regions: {top_regions}")

    # Filter akas to only these regions
    regional_akas = (
        akas_df.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter out non-scripted shows
    basics_scripted = basics_df.filter(
        (F.col("titleType") == "tvSeries")
        & ~F.array_contains(F.split(F.col("genres"), ","), "News")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Talk-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Game-Show")
        & ~F.array_contains(F.split(F.col("genres"), ","), "Reality-TV")
    )

    # Join episodes with ratings
    episodes_with_ratings = episodes_df.join(ratings_df, "tconst")

    # Calculate average rating by seasons
    season_ratings = episodes_with_ratings.groupBy("parentTconst", "seasonNumber").agg(
        F.avg("averageRating").alias("season_avg_rating")
    )

    # Count number of seasons
    season_counts = season_ratings.groupBy("parentTconst").agg(
        F.max("seasonNumber").alias("max_season")
    )

    # Filter series with more than min_seasons
    long_series = season_counts.filter(F.col("max_season") > min_seasons)

    # Filter ratings only for long series
    long_series_ratings = season_ratings.join(long_series, "parentTconst")

    # Use window function to get previous season's rating
    window_spec = Window.partitionBy("parentTconst").orderBy("seasonNumber")

    ratings_with_prev = long_series_ratings.withColumn(
        "prev_season_rating", F.lag("season_avg_rating", 1).over(window_spec)
    ).withColumn(
        "rating_diff", F.col("season_avg_rating") - F.col("prev_season_rating")
    )

    # Add series titles and regional data
    result = (
        ratings_with_prev.join(
            basics_scripted.select("tconst", "primaryTitle"),
            ratings_with_prev.parentTconst == basics_scripted.tconst,
        )
        .join(regional_akas, basics_scripted.tconst == regional_akas.tconst)
        .select(
            "region",
            "primaryTitle",
            "seasonNumber",
            "season_avg_rating",
            "prev_season_rating",
            "rating_diff",
        )
        .filter(F.col("rating_diff").isNotNull())
    )

    result_pd = result.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region]

            if not region_data.empty and len(region_data) > 0:
                # Show distribution of rating changes
                ax.hist(
                    region_data["rating_diff"],
                    bins=30,
                    color="lightcoral",
                    edgecolor="black",
                    alpha=0.7,
                )
                ax.axvline(
                    0, color="red", linestyle="--", linewidth=2, label="No change"
                )
                ax.set_title(
                    f"Season Rating Changes in {region}\n(Scripted Series Only)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Rating Difference", fontsize=10)
                ax.set_ylabel("Frequency", fontsize=10)
                ax.legend()
                ax.grid(True, alpha=0.3)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Season Rating Changes in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"season_rating_diff_regional_{min_seasons}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return result


def hook_shows(dataframes, save_path=".", top_n_regions=5):
    """
    Визначення серіалів, у яких рейтинг фіналу сезону 1 > рейтингу пілота.
    (Серіали, що "зачепили" глядача )
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing hook shows for regions: {top_regions}")

    # Filter akas to only these regions and get unique title-region pairs
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter basics by region (optional, but good for optimization)
    basics = filter_by_region(basics, akas, top_n_regions)

    series = basics.filter(F.col("titleType") == "tvSeries").select(
        "tconst", "primaryTitle"
    )

    # Join series with regional_akas to associate series with regions
    series_regional = series.join(regional_akas, "tconst")

    season1_eps = episodes.filter(F.col("seasonNumber") == 1).select(
        "tconst", "parentTconst", "episodeNumber"
    )

    eps_with_ratings = season1_eps.join(ratings, "tconst")

    # Correct logic using Window functions
    w = Window.partitionBy("parentTconst").orderBy("episodeNumber")

    agg = (
        eps_with_ratings.withColumn("first_rating", F.first("averageRating").over(w))
        .withColumn("last_rating", F.last("averageRating").over(w))
        .withColumn("first_ep", F.min("episodeNumber").over(w))
        .withColumn("last_ep", F.max("episodeNumber").over(w))
        .select("parentTconst", "first_rating", "last_rating", "first_ep", "last_ep")
        .distinct()
    )

    agg = agg.withColumn("delta_rating", F.col("last_rating") - F.col("first_rating"))

    growing = agg.filter(F.col("delta_rating") > 0)

    # Join with series_regional to get titles and regions
    result = (
        growing.join(
            series_regional, series_regional["tconst"] == growing["parentTconst"]
        )
        .select("region", "primaryTitle", "first_rating", "last_rating", "delta_rating")
        .orderBy("region", F.desc("delta_rating"))
    )

    result_pd = result.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 6 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].head(
                20
            )  # Top 20 per region

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="primaryTitle",
                    y="delta_rating",
                    hue="primaryTitle",
                    palette="viridis",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(f"Top 'Hook' Shows in {region}")
                ax.set_xlabel("TV Series")
                ax.set_ylabel("Rating Growth (Final - Pilot)")
                ax.tick_params(axis="x", rotation=45)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Top 'Hook' Shows in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "hook_shows_regional.png"))
        plt.show()

    return result


def sophomore_slump(dataframes, save_path=".", top_n_regions=5):
    """
    Аналіз "синдрому другого сезону":
    середня зміна рейтингу між фіналом 1-го та 2-го сезонів для серіалів із 3+ сезонами.
    """

    os.makedirs(save_path, exist_ok=True)

    basics = dataframes["title.basics"]
    episodes = dataframes["title.episode"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]

    # Filter basics by region
    basics = filter_by_region(basics, akas, top_n_regions)

    eps = episodes.filter(
        F.col("seasonNumber").isNotNull() & F.col("episodeNumber").isNotNull()
    ).select("tconst", "parentTconst", "seasonNumber", "episodeNumber")

    eps = eps.join(ratings.select("tconst", "averageRating"), "tconst")

    w = Window.partitionBy("parentTconst", "seasonNumber")
    season_finals = (
        eps.withColumn("max_ep", F.max("episodeNumber").over(w))
        .filter(F.col("episodeNumber") == F.col("max_ep"))
        .select("parentTconst", "seasonNumber", "averageRating")
    )

    valid_series = (
        season_finals.groupBy("parentTconst")
        .agg(F.countDistinct("seasonNumber").alias("num_seasons"))
        .filter(F.col("num_seasons") >= 3)
    )

    season_finals = season_finals.join(valid_series, "parentTconst")

    season_subset = season_finals.filter(F.col("seasonNumber").isin([1, 2]))

    pivoted = (
        season_subset.groupBy("parentTconst")
        .pivot("seasonNumber")
        .agg(F.first("averageRating"))
        .withColumnRenamed("1", "rating_s1_final")
        .withColumnRenamed("2", "rating_s2_final")
    )

    diff = pivoted.withColumn(
        "delta", F.col("rating_s2_final") - F.col("rating_s1_final")
    )

    result = diff.join(
        basics.select("tconst", "primaryTitle", "genres"),
        diff["parentTconst"] == basics["tconst"],
        "left",
    ).select("primaryTitle", "genres", "rating_s1_final", "rating_s2_final", "delta")

    genre_stats = (
        result.withColumn("main_genre", F.split(F.col("genres"), ",")[0])
        .groupBy("main_genre")
        .agg(
            F.avg("delta").alias("avg_delta"),
            F.count("primaryTitle").alias("num_series"),
        )
        .filter(F.col("num_series") >= 5)
        .orderBy("avg_delta")
    )

    genre_pd = genre_stats.toPandas()

    if not genre_pd.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(data=genre_pd, x="main_genre", y="avg_delta", palette="coolwarm")
        plt.title("'Синдром другого сезону' — середня зміна рейтингу між 1 і 2 сезоном")
        plt.xlabel("Жанр")
        plt.ylabel("Δ рейтинг (2 сезон - 1 сезон)")
        plt.axhline(0, color="gray", linestyle="--", alpha=0.7)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "sophomore_slump.png"))
        plt.show()

    return result, genre_stats
