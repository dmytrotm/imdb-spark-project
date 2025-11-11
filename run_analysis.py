"""
Business analysis execution module
Orchestrates all business questions and their visualizations
"""
from analysis import (
    top_genres_after_2010,
    avg_rating_long_series,
    season_rating_diff,
    avg_rating_by_actor,
    young_actors_2000s,
    top_directors_high_rated_films
)
from visualization import (
    plot_top_genres,
    plot_series_ratings,
    plot_season_trends,
    plot_actor_ratings,
    plot_young_actors_distribution,
    plot_director_stats
)


def run_genre_analysis(dataframes):
    """
    Execute genre analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[1/6] Genre analysis...")
    result = top_genres_after_2010(dataframes)
    plot_top_genres(result)


def run_series_analysis(dataframes):
    """
    Execute long series analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[2/6] Long series analysis...")
    result = avg_rating_long_series(dataframes)
    
    # Prepare full series DataFrame for visualization
    basics_df = dataframes['title.basics']
    episodes_df = dataframes['title.episode']
    ratings_df = dataframes['title.ratings']
    
    episode_counts = episodes_df.groupBy("parentTconst").agg({"*": "count"}).withColumnRenamed("count(1)", "episode_count")
    long_series = episode_counts.filter("episode_count > 50")
    series_info = long_series.join(basics_df.filter("titleType = 'tvSeries'"), 
                                  long_series.parentTconst == basics_df.tconst, "inner")
    series_with_ratings = series_info.join(ratings_df, 
                                          series_info.tconst == ratings_df.tconst, "inner")
    plot_series_ratings(series_with_ratings)


def run_season_trends_analysis(dataframes):
    """
    Execute season rating dynamics analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[3/6] Season rating dynamics analysis...")
    result = season_rating_diff(dataframes)
    plot_season_trends(result)


def run_actor_analysis(dataframes):
    """
    Execute actor analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[4/6] Actor analysis...")
    result = avg_rating_by_actor(dataframes)
    plot_actor_ratings(result)


def run_young_actors_analysis(dataframes):
    """
    Execute young actors age distribution analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[5/6] Young actors analysis...")
    result = young_actors_2000s(dataframes)
    plot_young_actors_distribution(result)


def run_director_analysis(dataframes):
    """
    Execute director analysis with visualization
    
    Args:
        dataframes: Dictionary of Spark DataFrames
    """
    print("\n[6/6] Director analysis...")
    result = top_directors_high_rated_films(dataframes)
    plot_director_stats(result)


def run_all_analyses(dataframes):
    """
    Execute all business analyses with error handling
    
    Args:
        dataframes: Dictionary of Spark DataFrames loaded from IMDB dataset
        
    Returns:
        dict: Dictionary with analysis results and status for each analysis
    """
    print("\n" + "="*80)
    print("STARTING BUSINESS ANALYSIS AND VISUALIZATION")
    print("="*80)
    
    results = {}
    
    # 1. Genre analysis
    try:
        run_genre_analysis(dataframes)
        results['genre_analysis'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during genre analysis: {e}")
        results['genre_analysis'] = {'status': 'failed', 'error': str(e)}
    
    # 2. Long series analysis
    try:
        run_series_analysis(dataframes)
        results['series_analysis'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during long series analysis: {e}")
        results['series_analysis'] = {'status': 'failed', 'error': str(e)}
    
    # 3. Season rating dynamics
    try:
        run_season_trends_analysis(dataframes)
        results['season_trends'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during season rating dynamics analysis: {e}")
        results['season_trends'] = {'status': 'failed', 'error': str(e)}
    
    # 4. Actor analysis
    try:
        run_actor_analysis(dataframes)
        results['actor_analysis'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during actor analysis: {e}")
        results['actor_analysis'] = {'status': 'failed', 'error': str(e)}
    
    # 5. Young actors
    try:
        run_young_actors_analysis(dataframes)
        results['young_actors'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during young actors analysis: {e}")
        results['young_actors'] = {'status': 'failed', 'error': str(e)}
    
    # 6. Top directors
    try:
        run_director_analysis(dataframes)
        results['director_analysis'] = {'status': 'success'}
    except Exception as e:
        print(f"Error during director analysis: {e}")
        results['director_analysis'] = {'status': 'failed', 'error': str(e)}
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETED")
    print("="*80)
    
    # Print summary
    successful = sum(1 for r in results.values() if r['status'] == 'success')
    failed = sum(1 for r in results.values() if r['status'] == 'failed')
    
    print(f"\n Summary: {successful} successful, {failed} failed")
    
    return results

