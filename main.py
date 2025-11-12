from pyspark.sql import SparkSession
from utils.reader import read_data
from analysis import (
    describe_dataframe,
    directors_increasing_ratings_trend,
    genre_popularity_trend,
    genre_actor_cyclicality,
    genre_duration_rating_analysis,
    genre_evolution_analysis,
    genre_combinations_analysis,
    writer_director_collaboration,
    actor_director_collaboration,
    top_directors_by_high_rating_and_votes,
    director_career_span_analysis,
    correlation_seasons_rating,
    top_episodes_by_votes_and_rating,
    genre_seasons_influence,
    avg_rating_long_series,
    season_rating_diff,
    actors_demography_stats,
    avg_rating_by_actor,
    young_actors_2000s,
    fading_stars, 
    rising_stars,
    underrated_genre_combos,
    hook_shows, 
    sophomore_slump,
    localization_gaps
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
    

    
    # print("\nAnalyzing genre evolution (before/after 2010)...")
    # genre_evolution_analysis(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing average rating of long series (50+ episodes)...")
    # avg_rating_long_series(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing season rating differences...")
    # season_rating_diff(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing actors with highest average ratings...")
    # avg_rating_by_actor(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing young actors age distribution...")
    # young_actors_2000s(dataframes, save_path="visualizations")
    
    # print("\nAnalyzing genre combinations and their success...")
    # genre_combinations_analysis(dataframes, save_path="visualizations")

    
    # print("\nAnalyzing writer-director collaborations...")
    # writer_director_collaboration(dataframes, save_path="visualizations")
    
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
    print("\nAnalyzing rising stars...")
    rising_stars(dataframes=dataframes,save_path="visualizations")
    
    print("\nAnalyzing hook shows...")
    hook_shows(dataframes=dataframes,save_path="visualizations")
    
    print("\nAnalyzing localization gaps...")
    localization_gaps(dataframes=dataframes,save_path="visualizations")
    
    print("\nAnalyzing fading stars...")
    fading_stars(dataframes=dataframes,save_path="visualizations")
    
    print("\nAnalyzing underrated genre combos...")
    underrated_genre_combos(dataframes=dataframes,save_path="visualizations")
    
    print("\nAnalyzing sophomore slump...")
    sophomore_slump(dataframes=dataframes,save_path="visualizations")



    # print("\n" + "="*50)
    # print("All analyses complete. Visualizations saved to 'visualizations' directory.")
    # print("="*50)

    spark.stop()

if __name__ == "__main__":
    main()