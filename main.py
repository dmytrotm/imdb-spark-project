from pyspark.sql import SparkSession
from utils.reader import read_data
from analysis import (
    describe_dataframe,
    directors_increasing_ratings_trend,
    genre_popularity_trend,
    genre_actor_cyclicality,
    genre_duration_rating_analysis,
    writer_director_collaboration,
    actor_director_collaboration,
    top_directors_by_high_rating_and_votes,
    director_career_span_analysis,
    correlation_seasons_rating,
    top_episodes_by_votes_and_rating,
    genre_seasons_influence,
    actors_demography_stats
)

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    # Read the data
    dataframes = read_data(spark)

    # # --- General Data Description ---
    # print("="*50)
    # print("General Dataframe Descriptions")
    # print("="*50)
    # for name, df in dataframes.items():
    #     describe_dataframe(df, name)

    # --- Run All Analyses ---
    print("\n" + "="*50)
    print("Running Analyses")
    print("="*50)
    
    print("\nAnalyzing writer-director collaborations...")
    writer_director_collaboration(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing actor-director collaborations...")
    # actor_director_collaboration(dataframes, save_path="visualizations")

    # print("\nAnalyzing directors with increasing ratings trend...")
    # directors_increasing_ratings_trend(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing director career spans...")
    # director_career_span_analysis(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing top directors by high-rated films...")
    # top_directors_by_high_rating_and_votes(dataframes, save_path="visualizations")

    # print("\nAnalyzing genre popularity trends...")
    # genre_popularity_trend(dataframes, save_path="visualizations")

    # print("\nAnalyzing genre and actor cyclicality...")
    # genre_actor_cyclicality(dataframes, save_path="visualizations")

    # print("\nAnalyzing genre duration and rating...")
    # genre_duration_rating_analysis(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing TV series seasons vs. rating correlation...")
    # correlation_seasons_rating(dataframes, save_path="visualizations")

    # print("\nAnalyzing top TV episodes by votes...")
    # top_episodes_by_votes_and_rating(dataframes, save_path="visualizations")

    # print("\nAnalyzing influence of genre on TV series seasons...")
    # genre_seasons_influence(dataframes, save_path="visualizations")

    # print("\nAnalyzing actor demographics and career statistics...")
    # actors_demography_stats(dataframes, save_path="visualizations")

    # print("\n" + "="*50)
    # print("All analyses complete. Visualizations saved to 'visualizations' directory.")
    # print("="*50)

    spark.stop()

if __name__ == "__main__":
    main()