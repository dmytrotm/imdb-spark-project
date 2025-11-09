from pyspark.sql import SparkSession
from utils.reader import read_data
from functions.business_questions_1 import writers_directors_collaboration_trend, top_directors_by_high_rating_and_votes, correlation_seasons_rating, top_episodes_by_votes_and_rating, actors_demography_stats, genre_seasons_influence

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    # Read the data
    dataframes = read_data(spark)

    # # Analyze each dataframe
    # for name, df in dataframes.items():
    #     describe_dataframe(df, name)

    print("1. Соціальна демографія акторів (топ-30):")
    demography = actors_demography_stats(dataframes)
    demography.show(truncate=False)

    print("2. Вплив жанрового різноманіття на кількість сезонів серіалів:")
    genre_seasons = genre_seasons_influence(dataframes)
    genre_seasons.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()