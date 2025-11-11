# Implemented Business Analyses

This document outlines the different data analyses implemented in this project.

---

## Collaboration Analysis
**File:** `analysis/collaboration_analysis.py`

### 1. Writer-Director Collaborations
- **Function:** `writer_director_collaboration`
- **Business Question:** Which writer-director duos are most prolific, and do their joint films improve in quality over time or with each subsequent collaboration?
- **Description:** This analysis identifies the most frequent collaborations between writers and directors. It provides three visualizations:
    1. A bar chart of the top 10 most frequent writer-director pairs.
    2. A line plot showing the rating trend of their joint films by year.
    3. A line plot analyzing if the rating of their collaborations improves sequentially (from their 1st to Nth film together).

### 2. Actor-Director Collaborations
- **Function:** `actor_director_collaboration`
- **Business Question:** Which actor-director pairings are most common, and are their collaborations critically successful?
- **Description:** This analysis finds the most frequent collaborations between actors and directors. It visualizes the top 10 pairs and tracks the rating trends of their joint films by year.

---

## Director Analysis
**File:** `analysis/director_analysis.py`

### 1. Director Rating Trends
- **Function:** `directors_increasing_ratings_trend`
- **Business Question:** Which directors are on an upward trajectory in terms of film quality, making them promising talents for future investment?
- **Description:** Identifies directors whose films have shown a trend of increasing average ratings over the past 10 years.

### 2. Top Directors by High-Rated Films
- **Function:** `top_directors_by_high_rating_and_votes`
- **Business Question:** Who are the most consistently successful and popular directors over time?
- **Description:** This analysis finds directors who have the largest number of films with a rating above 8 and shows how the average number of votes for their films has changed over the years.

### 3. Director Career Span Analysis
- **Function:** `director_career_span_analysis`
- **Business Question:** How do a director's film ratings evolve over their entire career, and is there a common "peak" period for creative output?
- **Description:** This analysis visualizes the career-long rating trajectories of prolific directors. It identifies directors based on career length and number of films, then plots their average film rating against their "career year" (i.e., years since their first film). The filtering criteria are customizable.
- **Parameters:**
    - `min_career_length` (default: 20): Minimum career duration in years to be included.
    - `min_film_count` (default: 10): Minimum number of films directed.
    - `num_directors_to_plot` (default: 7): The number of top directors to include in the plot.

---

## Genre Analysis
**File:** `analysis/genre_analysis.py`

### 1. Genre Popularity Trends
- **Function:** `genre_popularity_trend`
- **Business Question:** Which genres are gaining popularity in different markets, and does this popularity correlate with higher ratings?
- **Description:** Analyzes the increase in average votes and ratings for different genres over the past decade across various regions.

### 2. Cyclicality of Genres and Actors
- **Function:** `genre_actor_cyclicality`
- **Business Question:** Are there predictable cycles in genre and actor popularity that can inform production and marketing schedules?
- **Description:** Investigates whether the popularity of certain genres and actors follows a cyclical pattern in different countries over the years.

### 3. Genre Duration vs. Rating
- **Function:** `genre_duration_rating_analysis`
- **Business Question:** Does the length of a film impact its critical success within different genres?
- **Description:** Examines the average film duration (runtime) for different genres and analyzes its correlation with the average rating.

---

## TV Series Analysis
**File:** `analysis/tv_analysis.py`

### 1. TV Series Seasons vs. Rating
- **Function:** `correlation_seasons_rating`
- **Business Question:** Do longer-running TV series tend to be higher or lower-rated?
- **Description:** Analyzes if there is a correlation between the number of seasons a TV series has and its average rating.

### 2. Top TV Episodes
- **Function:** `top_episodes_by_votes_and_rating`
- **Business Question:** What are the most talked-about and popular episodes of all time?
- **Description:** Identifies the most popular individual TV episodes based on the total number of votes they have received.

### 3. Genre Influence on Seasons
-_Function:** `genre_seasons_influence`
- **Business Question:** Do certain genres tend to produce longer-running series?
- **Description:** Analyzes how the genre of a TV series influences the number of seasons it has.

---

## Actor Analysis
**File:** `analysis/actor_analysis.py`

### 1. Actor Demographics and Statistics
- **Function:** `actors_demography_stats`
- **Business Question:** What is the profile of the most active and successful actors?
- **Description:** Calculates demographic and career statistics for actors, including their age, the number of titles they have appeared in, and their average film rating.
