from pyspark.sql import SparkSession
from utils.reader import read_data
from analysis import describe_dataframe, directors_increasing_ratings_trend, genre_popularity_trend, genre_actor_cyclicality, genre_duration_rating_analysis, writer_director_collaboration

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    # Read the data
    dataframes = read_data(spark)

    # Analyze each dataframe
    for name, df in dataframes.items():
        describe_dataframe(df, name)

    print("Analyzing writer-director collaborations...")
    writer_director_collaboration(dataframes, save_path="visualizations")

    print("Analyzing directors with increasing ratings trend...")
    directors_increasing_ratings_trend(dataframes, save_path="visualizations")

    print("Analyzing genre popularity trends...")
    genre_popularity_trend(dataframes, save_path="visualizations")

    print("Analyzing genre and actor cyclicality...")
    genre_actor_cyclicality(dataframes, save_path="visualizations")

    print("Analyzing genre duration and rating...")
    genre_duration_rating_analysis(dataframes, save_path="visualizations")

    spark.stop()

if __name__ == "__main__":
    main()