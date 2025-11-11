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
