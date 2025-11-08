from pyspark.sql import SparkSession
from utils.reader import read_data
from utils.analysis import describe_dataframe
from functions.business_questions_1 import writers_directors_collaboration_trend, top_directors_by_high_rating_and_votes, correlation_seasons_rating, top_episodes_by_votes_and_rating

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    # Read the data
    dataframes = read_data(spark)

    # # Analyze each dataframe
    # for name, df in dataframes.items():
    #     describe_dataframe(df, name)

    print("1. Топ сценаристи-режисери:")
    collab = writers_directors_collaboration_trend(dataframes)
    collab.show(truncate=False)

    print("2. Режисери з фільмами рейтингом >8 та голосами по роках:")
    directors_votes = top_directors_by_high_rating_and_votes(dataframes)
    directors_votes.show(50)

    print("3. Кореляція кількості сезонів із рейтингом:")
    corr = correlation_seasons_rating(dataframes)
    corr.show()

    print("4. Топ TV-епізодів за кількістю голосів і рейтингом:")
    top_eps = top_episodes_by_votes_and_rating(dataframes)
    top_eps.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()