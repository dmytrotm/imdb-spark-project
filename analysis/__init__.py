from .director_analysis import top_directors_high_rated_films
from .genre_analysis import top_genres_after_2010
from .collaboration_analysis import avg_rating_by_actor, young_actors_2000s
from .general import avg_rating_long_series, season_rating_diff

__all__ = [
    "top_directors_high_rated_films",
    "top_genres_after_2010",
    "avg_rating_by_actor",
    "young_actors_2000s",
    "avg_rating_long_series",
    "season_rating_diff",
]
