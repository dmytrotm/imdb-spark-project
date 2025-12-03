import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql import Window
from analysis.general import filter_by_region, get_top_regions


def actors_demography_stats(dataframes, save_path=".", top_n_regions=5):
    """
    Соціальна демографія акторів: вік (birthYear), активність (уникальна кількість фільмів), середній рейтинг
    Створює 3 окремі візуалізації для кращого розуміння:
    1. Топ-10 найактивніших акторів по регіонах (bar chart)
    2. Топ-10 акторів з найвищим рейтингом по регіонах (bar chart)
    3. Розподіл віку топ-акторів по регіонах (box plot)
    """
    os.makedirs(save_path, exist_ok=True)

    # Таблиці
    name_basics = dataframes["name.basics"]
    principals = dataframes["title.principals"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]
    basics = dataframes["title.basics"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing actor demographics for regions: {top_regions}")

    # Filter akas to only these regions and get unique title-region pairs
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter for movies only
    movies = basics.filter(F.col("titleType") == "movie").select("tconst")

    # Беремо тільки акторів та актрис
    actors = principals.filter(F.col("category").isin(["actor", "actress"]))

    # Join with movies and regional_akas to get region info
    actors_regional = actors.join(movies, "tconst").join(regional_akas, "tconst")

    # Обчислюємо унікальні пари актор-фільм-регіон
    distinct_works = actors_regional.select("region", "nconst", "tconst").distinct()

    # Підрахунок унікальної кількості фільмів на актора per region
    film_counts = distinct_works.groupBy("region", "nconst").agg(
        F.count("tconst").alias("num_titles")
    )

    # Об'єднуємо з інформацією про акторів
    actors_info = film_counts.join(
        name_basics.select("nconst", "primaryName", "birthYear"), "nconst"
    ).filter(
        F.col("birthYear").isNotNull()
    )  # Filter out null birth years

    # Обчислюємо середній рейтинг для кожного актора per region
    actors_films = distinct_works.join(ratings, "tconst")
    avg_ratings = actors_films.groupBy("region", "nconst").agg(
        F.avg("averageRating").alias("avg_rating")
    )

    # Об'єднуємо всі дані
    actors_info = actors_info.join(avg_ratings, ["region", "nconst"])

    # Calculate age (approximate, using 2024 as reference)
    actors_info = actors_info.withColumn("age", F.lit(2024) - F.col("birthYear"))

    # Filter actors with minimum activity (at least 5 films in region)
    actors_info = actors_info.filter(F.col("num_titles") >= 5)

    result_pd = actors_info.toPandas()

    if not result_pd.empty:
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        # ===== VISUALIZATION 1: Top 10 Most Active Actors by Region =====
        fig1, axes1 = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes1 = axes1.flatten() if num_regions > 1 else [axes1]

        for i, region in enumerate(top_regions):
            ax = axes1[i]
            region_data = result_pd[result_pd["region"] == region].nlargest(
                10, "num_titles"
            )

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="num_titles",
                    y="primaryName",
                    hue="primaryName",
                    palette="Blues_r",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(
                    f"Top 10 Most Active Actors in {region}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Number of Films", fontsize=10)
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Top 10 Most Active Actors in {region}")

        for j in range(i + 1, len(axes1)):
            axes1[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "actor_activity_by_region.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        # ===== VISUALIZATION 2: Top 10 Highest Rated Actors by Region =====
        fig2, axes2 = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes2 = axes2.flatten() if num_regions > 1 else [axes2]

        for i, region in enumerate(top_regions):
            ax = axes2[i]
            # Filter actors with at least 10 films for more reliable ratings
            region_data = result_pd[
                (result_pd["region"] == region) & (result_pd["num_titles"] >= 10)
            ].nlargest(10, "avg_rating")

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="avg_rating",
                    y="primaryName",
                    hue="primaryName",
                    palette="Greens_r",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(
                    f"Top 10 Highest Rated Actors in {region}\n(min. 10 films)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_ylabel("")
                ax.set_xlim(0, 10)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Top 10 Highest Rated Actors in {region}")

        for j in range(i + 1, len(axes2)):
            axes2[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "actor_quality_by_region.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        # ===== VISUALIZATION 3: Age Distribution of Top Actors by Region =====
        fig3, axes3 = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes3 = axes3.flatten() if num_regions > 1 else [axes3]

        for i, region in enumerate(top_regions):
            ax = axes3[i]
            # Get top 30 actors by activity for age analysis
            region_data = result_pd[result_pd["region"] == region].nlargest(
                30, "num_titles"
            )

            if not region_data.empty:
                # Box plot with individual points
                sns.boxplot(
                    data=region_data, y="age", color="lightblue", ax=ax, width=0.3
                )
                sns.stripplot(
                    data=region_data,
                    y="age",
                    color="darkblue",
                    alpha=0.6,
                    size=8,
                    ax=ax,
                )

                ax.set_title(
                    f"Age Distribution of Top 30 Actors in {region}",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_ylabel("Age (years)", fontsize=10)
                ax.set_xlabel("")

                # Add statistics text
                mean_age = region_data["age"].mean()
                median_age = region_data["age"].median()
                ax.text(
                    0.98,
                    0.98,
                    f"Mean: {mean_age:.1f}\nMedian: {median_age:.1f}",
                    transform=ax.transAxes,
                    fontsize=10,
                    verticalalignment="top",
                    horizontalalignment="right",
                    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.7),
                )
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Age Distribution in {region}")

        for j in range(i + 1, len(axes3)):
            axes3[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, "actor_age_distribution_by_region.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return actors_info


def avg_rating_by_actor(
    datasets, save_path=".", min_films=10, top_n=20, top_n_regions=5
):
    """
    Analyzes average film ratings for actors with a minimum number of films per region.
    Identifies actors associated with high-quality productions for casting decisions.

    Args:
        datasets (dict): A dictionary of Spark DataFrames.
        save_path (str): The path to save the visualizations.
        min_films (int): Minimum number of films. Defaults to 10.
        top_n (int): Number of top actors to show per region. Defaults to 20.
        top_n_regions (int): Number of top regions to analyze. Defaults to 5.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with actor statistics including average ratings and film counts.
    """
    os.makedirs(save_path, exist_ok=True)

    print("\n" + "=" * 80)
    print(
        f"BUSINESS QUESTION: Actors with highest average film rating (min. {min_films} films) by region"
    )
    print("=" * 80)

    basics_df = datasets["title.basics"]
    principals_df = datasets["title.principals"]
    ratings_df = datasets["title.ratings"]
    names_df = datasets["name.basics"]
    akas_df = datasets["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas_df, top_n_regions)
    print(f"Analyzing top actors for regions: {top_regions}")

    # Filter akas to only these regions and get unique title-region pairs
    regional_akas = (
        akas_df.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    # Filter only movies
    movies = basics_df.filter(F.col("titleType") == "movie").select("tconst")

    # Filter only actors and actresses
    actors = principals_df.filter(
        (F.col("category") == "actor") | (F.col("category") == "actress")
    )

    # Join movies with actors and regional_akas
    movies_actors_regional = (
        actors.join(movies, "tconst").join(regional_akas, "tconst")
    )

    # Join with ratings
    movies_actors_ratings = movies_actors_regional.join(
        ratings_df.select("tconst", "averageRating", "numVotes"), "tconst"
    )

    # Group by region and actor, calculate average rating
    actor_stats = (
        movies_actors_ratings.groupBy("region", "nconst")
        .agg(
            F.avg("averageRating").alias("avg_rating"),
            F.count("*").alias("film_count"),
            F.sum("numVotes").alias("total_votes"),
        )
        .filter(F.col("film_count") >= min_films)
    )

    # Add actor names
    actor_stats_with_names = actor_stats.join(
        names_df.select("nconst", "primaryName", "birthYear"), "nconst", "inner"
    )

    # Get top actors per region
    w_rank = Window.partitionBy("region").orderBy(F.desc("avg_rating"))
    top_actors = (
        actor_stats_with_names.withColumn("rank", F.rank().over(w_rank))
        .filter(F.col("rank") <= top_n)
        .orderBy("region", "rank")
    )

    result_pd = top_actors.toPandas()

    if not result_pd.empty:
        # Create subplots - one per region
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].sort_values(
                "avg_rating", ascending=False
            )

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="avg_rating",
                    y="primaryName",
                    hue="primaryName",
                    palette="Greens_r",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(
                    f"Top {top_n} Actors by Rating in {region}\n(min. {min_films} films)",
                    fontsize=12,
                    fontweight="bold",
                )
                ax.set_xlabel("Average Rating", fontsize=10)
                ax.set_ylabel("")
                ax.set_xlim(0, 10)
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")
                ax.set_title(f"Top Actors in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"top_actors_by_rating_regional_{min_films}.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

    return top_actors


def young_actors_2000s(datasets, save_path=".", years_back=5, top_n_regions=5):
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

    print("\n" + "=" * 80)
    print(
        f"BUSINESS QUESTION: Actor age distribution in films {start_year}-{current_year}"
    )
    print("=" * 80)

    basics_df = datasets["title.basics"]
    principals_df = datasets["title.principals"]
    names_df = datasets["name.basics"]
    akas_df = datasets["title.akas"]

    # Filter movies by region
    basics_df = filter_by_region(basics_df, akas_df, top_n_regions)

    # Filter films from the last N years
    recent_movies = basics_df.filter(
        (F.col("titleType") == "movie")
        & (F.col("startYear") >= start_year)
        & (F.col("startYear") <= current_year)
    )

    # Filter actors/actresses
    actors = principals_df.filter(
        (F.col("category") == "actor") | (F.col("category") == "actress")
    )

    # Join films with actors
    movies_actors = recent_movies.join(
        actors, recent_movies.tconst == actors.tconst, "inner"
    ).drop(actors.tconst)

    # Join with actor information
    actors_info = movies_actors.join(
        names_df.filter(F.col("birthYear").isNotNull()), "nconst", "inner"
    )

    # Calculate actor's age at film release
    actors_with_age = actors_info.withColumn(
        "age_at_release", F.col("startYear") - F.col("birthYear")
    ).filter((F.col("age_at_release") >= 18) & (F.col("age_at_release") <= 100))

    # Create age groups
    actors_with_age = actors_with_age.withColumn(
        "age_group",
        F.when(
            (F.col("age_at_release") >= 18) & (F.col("age_at_release") <= 25),
            "18-25 (Youth)",
        )
        .when(
            (F.col("age_at_release") >= 26) & (F.col("age_at_release") <= 35),
            "26-35 (Prime)",
        )
        .when(
            (F.col("age_at_release") >= 36) & (F.col("age_at_release") <= 45),
            "36-45 (Mature)",
        )
        .when(
            (F.col("age_at_release") >= 46) & (F.col("age_at_release") <= 55),
            "46-55 (Experienced)",
        )
        .otherwise("56+ (Veterans)"),
    )

    # Group by age categories
    age_distribution = (
        actors_with_age.groupBy("age_group")
        .agg(
            F.countDistinct("nconst").alias("unique_actors"),
            F.count("*").alias("total_roles"),
            F.avg("age_at_release").alias("avg_age"),
        )
        .orderBy("avg_age")
    )

    print(f"\nActor distribution by age groups in films {start_year}-{current_year}:")
    age_distribution.select(
        "age_group",
        "unique_actors",
        "total_roles",
        F.round("avg_age", 1).alias("avg_age"),
    ).show(truncate=False)

    # Additional: trend by years
    print("\nTrend by years (average actor age):")
    yearly_trend = (
        actors_with_age.groupBy("startYear")
        .agg(
            F.avg("age_at_release").alias("avg_age"),
            F.countDistinct("nconst").alias("unique_actors"),
        )
        .orderBy("startYear")
    )

    yearly_trend.select(
        "startYear", F.round("avg_age", 1).alias("avg_age"), "unique_actors"
    ).show()

    # Visualization
    age_dist_pd = age_distribution.toPandas()
    yearly_trend_pd = yearly_trend.toPandas()

    if not age_dist_pd.empty:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Age group distribution (unique actors)
        axes[0, 0].bar(
            age_dist_pd["age_group"], age_dist_pd["unique_actors"], color="steelblue"
        )
        axes[0, 0].set_title(
            f"Unique Actors by Age Group ({start_year}-{current_year})"
        )
        axes[0, 0].set_xlabel("Age Group")
        axes[0, 0].set_ylabel("Number of Unique Actors")
        axes[0, 0].tick_params(axis="x", rotation=45)
        axes[0, 0].grid(True, alpha=0.3, axis="y")

        # 2. Total roles by age group
        axes[0, 1].bar(
            age_dist_pd["age_group"], age_dist_pd["total_roles"], color="coral"
        )
        axes[0, 1].set_title(f"Total Roles by Age Group ({start_year}-{current_year})")
        axes[0, 1].set_xlabel("Age Group")
        axes[0, 1].set_ylabel("Total Number of Roles")
        axes[0, 1].tick_params(axis="x", rotation=45)
        axes[0, 1].grid(True, alpha=0.3, axis="y")

        # 3. Average age trend by year
        if not yearly_trend_pd.empty:
            axes[1, 0].plot(
                yearly_trend_pd["startYear"],
                yearly_trend_pd["avg_age"],
                marker="o",
                linewidth=2,
                markersize=8,
                color="green",
            )
            axes[1, 0].set_title("Average Actor Age Trend Over Years")
            axes[1, 0].set_xlabel("Year")
            axes[1, 0].set_ylabel("Average Age")
            axes[1, 0].grid(True, alpha=0.3)

        # 4. Unique actors trend by year
        if not yearly_trend_pd.empty:
            axes[1, 1].bar(
                yearly_trend_pd["startYear"],
                yearly_trend_pd["unique_actors"],
                color="purple",
            )
            axes[1, 1].set_title("Number of Unique Actors by Year")
            axes[1, 1].set_xlabel("Year")
            axes[1, 1].set_ylabel("Unique Actors")
            axes[1, 1].grid(True, alpha=0.3, axis="y")

        plt.tight_layout()
        plt.savefig(
            os.path.join(save_path, f"young_actors_{start_year}_{current_year}.png"),
            dpi=300,
        )
        plt.show()

    return age_distribution


def rising_stars(dataframes, save_path=".", top_n_regions=5):
    """
    10 акторів із найшвидшим зростанням попиту (velocity росту голосів за фільми за 5 років)
    по кожному з топ-N регіонів.
    """

    os.makedirs(save_path, exist_ok=True)

    name_basics = dataframes["name.basics"]
    principals = dataframes["title.principals"]
    ratings = dataframes["title.ratings"]
    akas = dataframes["title.akas"]
    basics = dataframes["title.basics"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing rising stars for regions: {top_regions}")

    # Filter akas to only these regions and get unique title-region pairs
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    actors = principals.filter(F.col("category").isin(["actor", "actress"]))

    # Join everything: actors -> basics -> ratings -> regional_akas
    # Ensure we only look at movies
    works = (
        actors.join(
            basics.filter(F.col("titleType") == "movie").select("tconst", "startYear"),
            "tconst",
        )
        .join(ratings.select("tconst", "numVotes", "averageRating"), "tconst")
        .join(regional_akas, "tconst")
        .filter((F.col("startYear") >= 2018) & (F.col("startYear") <= 2023))
    )

    # Calculate avg votes AND avg rating per actor per year per region
    yearly_stats = works.groupBy("region", "nconst", "startYear").agg(
        F.avg("numVotes").alias("avg_votes"),
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("films_this_year"),
    )

    # Filter actors with > 1 year of data (per region) AND minimum total films in region
    window_count_years = Window.partitionBy("region", "nconst")
    yearly_stats_filtered = (
        yearly_stats.withColumn("num_years", F.count("*").over(window_count_years))
        .withColumn(
            "total_films_in_region", F.sum("films_this_year").over(window_count_years)
        )
        .filter((F.col("num_years") > 1) & (F.col("total_films_in_region") >= 3))
    )

    # Calculate velocity for both votes and ratings
    w = Window.partitionBy("region", "nconst").orderBy("startYear")
    stats_enriched = yearly_stats_filtered.withColumn(
        "prev_avg_votes", F.lag("avg_votes").over(w)
    ).withColumn("prev_avg_rating", F.lag("avg_rating").over(w))

    stats_enriched = stats_enriched.withColumn(
        "vote_velocity", F.col("avg_votes") - F.col("prev_avg_votes")
    ).withColumn("rating_trend", F.col("avg_rating") - F.col("prev_avg_rating"))

    # Calculate composite "rising star" score per actor per region
    # Score = avg(vote_velocity) * avg(rating) - we want both growing popularity AND quality
    actor_metrics = (
        stats_enriched.groupBy("region", "nconst")
        .agg(
            F.avg("vote_velocity").alias("avg_vote_velocity"),
            F.avg("rating_trend").alias("avg_rating_trend"),
            F.avg("avg_rating").alias("overall_avg_rating"),
        )
        .filter(F.col("avg_vote_velocity") > 0)
    )  # Must have positive vote growth

    # Composite score: vote velocity weighted by rating quality
    # Normalize vote velocity (divide by 1000) and multiply by rating
    actor_metrics = actor_metrics.withColumn(
        "rising_star_score",
        (F.col("avg_vote_velocity") / 1000) * F.col("overall_avg_rating")
        + F.col("avg_rating_trend") * 10,
    )

    # Rank top 10 per region by composite score
    w_rank = Window.partitionBy("region").orderBy(F.desc("rising_star_score"))
    top_actors = actor_metrics.withColumn("rank", F.rank().over(w_rank)).filter(
        F.col("rank") <= 10
    )

    # Join with names
    result = (
        top_actors.join(name_basics.select("nconst", "primaryName"), "nconst")
        .select(
            "region",
            "primaryName",
            "rising_star_score",
            "avg_vote_velocity",
            "avg_rating_trend",
            "overall_avg_rating",
            "rank",
        )
        .orderBy("region", "rank")
    )

    result_pd = result.toPandas()

    if not result_pd.empty:
        # Create subplots
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].sort_values(
                "rising_star_score", ascending=False
            )

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="rising_star_score",
                    y="primaryName",
                    hue="primaryName",
                    palette="viridis",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(f"Rising Stars in {region}")
                ax.set_xlabel("Rising Star Score (Popularity + Quality)")
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No data", ha="center", va="center")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "rising_stars_regional.png"))
        plt.show()

    return result


def fading_stars(dataframes, save_path=".", top_n_regions=5):
    """
    Аналіз 'згасаючих зірок' по регіонах:
    режисери або актори, які мали середній рейтинг >7.5 у 2000-х,
    але <5.5 у 2020-х.
    """

    os.makedirs(save_path, exist_ok=True)

    name_basics = dataframes["name.basics"]
    principals = dataframes["title.principals"]
    ratings = dataframes["title.ratings"]
    basics = dataframes["title.basics"]
    akas = dataframes["title.akas"]

    # Get top regions
    top_regions = get_top_regions(akas, top_n_regions)
    print(f"Analyzing fading stars for regions: {top_regions}")

    # Filter akas to only these regions and get unique title-region pairs
    regional_akas = (
        akas.filter(F.col("region").isin(top_regions))
        .select(F.col("titleId").alias("tconst"), "region")
        .distinct()
    )

    people = principals.filter(
        F.col("category").isin(["actor", "actress", "director"])
    ).select("tconst", "nconst", "category")

    # Join everything: people -> basics -> ratings -> regional_akas
    # Ensure we only look at movies
    joined = (
        people.join(
            basics.filter(F.col("titleType") == "movie").select("tconst", "startYear"),
            "tconst",
        )
        .join(ratings.select("tconst", "averageRating"), "tconst")
        .join(regional_akas, "tconst")
        .filter(F.col("startYear").isNotNull())
    )

    joined = joined.withColumn("decade", (F.col("startYear") / 10).cast("int") * 10)

    # Group by region, nconst, category, decade
    avg_by_decade = joined.groupBy("region", "nconst", "category", "decade").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("tconst").alias("film_count"),
    )

    # Filter: require at least 3 films per person per region (across all decades)
    window_region_person = Window.partitionBy("region", "nconst")
    avg_by_decade = avg_by_decade.withColumn(
        "total_films_in_region", F.sum("film_count").over(window_region_person)
    ).filter(F.col("total_films_in_region") >= 3)

    # Pivot decade
    decade_pivot = (
        avg_by_decade.groupBy("region", "nconst", "category")
        .pivot("decade")
        .agg(F.first("avg_rating"))
    )

    # Filter for fading stars
    # Check if columns exist (might not if no data for those decades)
    if "2000" in decade_pivot.columns and "2020" in decade_pivot.columns:
        fading = decade_pivot.filter(
            (F.col("2000").isNotNull())
            & (F.col("2020").isNotNull())
            & (F.col("2000") > 7.5)
            & (F.col("2020") < 5.5)
        )
    else:
        print("Not enough data for 2000s and 2020s comparison.")
        return None

    # Calculate rating decline score
    fading = fading.withColumn("decline_score", F.col("2000") - F.col("2020"))

    # Rank top 10 per region by decline score
    w_rank = Window.partitionBy("region").orderBy(F.desc("decline_score"))
    top_fading = fading.withColumn("rank", F.rank().over(w_rank)).filter(
        F.col("rank") <= 10
    )

    result = (
        top_fading.join(name_basics.select("nconst", "primaryName"), "nconst")
        .select(
            "region", "primaryName", "category", "2000", "2020", "decline_score", "rank"
        )
        .orderBy("region", "rank")
    )

    result_pd = result.toPandas()

    if not result_pd.empty:
        # Create subplots - same style as rising_stars
        num_regions = len(top_regions)
        cols = 2
        rows = (num_regions + 1) // 2

        fig, axes = plt.subplots(rows, cols, figsize=(15, 5 * rows))
        axes = axes.flatten() if num_regions > 1 else [axes]

        for i, region in enumerate(top_regions):
            ax = axes[i]
            region_data = result_pd[result_pd["region"] == region].sort_values(
                "decline_score", ascending=False
            )

            if not region_data.empty:
                sns.barplot(
                    data=region_data,
                    x="decline_score",
                    y="primaryName",
                    hue="primaryName",
                    palette="Reds_r",
                    legend=False,
                    ax=ax,
                )
                ax.set_title(f"Fading Stars in {region}")
                ax.set_xlabel("Rating Decline (2000s - 2020s)")
                ax.set_ylabel("")
            else:
                ax.text(0.5, 0.5, "No fading stars found", ha="center", va="center")
                ax.set_title(f"Fading Stars in {region}")

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis("off")

        plt.tight_layout()
        plt.savefig(os.path.join(save_path, "fading_stars_regional.png"))
        plt.show()

    return result
