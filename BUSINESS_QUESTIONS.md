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

### 4. Genre Evolution Analysis
- **Function:** `genre_evolution_analysis`
- **Business Question:** How have genre preferences evolved - which genres gained or lost popularity after 2010 compared to the decade before?
- **Description:** Identifies shifting audience preferences and market trends by comparing genre performance before and after 2010. This helps studios and investors understand:
  - Which genres are rising (emerging opportunities)
  - Which genres are declining (saturated markets)
  - Changes in audience quality expectations
  - ROI shifts per genre
- **Metrics:** Film volume growth %, rating quality change, engagement growth %, momentum score (composite)
- **Visualization:** 7 charts including momentum ranking, before/after comparisons, scatter plot of quality vs popularity evolution
- **Unique value:** Comparative analysis that reveals market evolution trends, not just current popularity snapshots

### 5. Genre Combinations Analysis
- **Function:** `genre_combinations_analysis`
- **Business Question:** Which combinations of genres are most successful and popular? What genre pairings lead to the highest ratings and audience engagement?
- **Description:** Analyzes multi-genre films to identify which genre combinations work best together. This information is valuable for:
  - Producers planning new projects and deciding on genre mix
  - Investors evaluating project proposals based on genre combination track record
  - Studios understanding which creative blends resonate with audiences
  - Marketing teams positioning films based on proven successful combinations
- **Metrics:** Average rating per combination, total votes, engagement score (rating × log(votes)), film count
- **Visualization:** 6 charts including top combinations by rating, popularity, engagement score, scatter plot of quality vs volume, and frequency analysis
- **Unique value:** First analysis to systematically examine genre synergies rather than individual genres, revealing which creative combinations maximize both critical and commercial success

### 6. Underrated Genre Combinations
- **Function:** `underrated_genre_combos`
- **Business Question:** Which genre combinations have high average ratings but low vote counts? This identifies 'underrated' niches that are critically well-received but not yet mainstream, pointing to potential market opportunities.
- **Description:** This analysis filters for genre combinations that have an average rating above a certain threshold (e.g., 7.5) but a total number of votes below another threshold. The resulting list shows high-quality, low-visibility genre mixes.
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
- **Function:** `genre_seasons_influence`
- **Business Question:** Do certain genres tend to produce longer-running series?
- **Description:** Analyzes how the genre of a TV series influences the number of seasons it has.

### 4. Average Rating of Long Series
- **Function:** `avg_rating_long_series`
- **Business Question:** What is the average rating of TV series with more than 50 episodes?
- **Description:** Provides insight into how audiences rate long-running TV series. This is important for TV networks and streaming services to evaluate project success and make decisions about extending series or creating new similar formats.

### 5. Season Rating Differences
- **Function:** `season_rating_diff`
- **Business Question:** For each TV series with more than 3 seasons, determine the difference in average rating between the current season and the previous one.
- **Description:** Tracks the dynamics of audience rating changes from season to season. Rating growth may indicate quality improvement or audience growth, decline - loss of interest. This information is critical for showrunners, writers, and producers to adjust the production process. Uses window functions (lag) to compare consecutive seasons.

### 6. Hook Shows (Hook Analysis)
- **Function:** `hook_shows`
- **Business Question:** Which series are best at 'hooking' new viewers?
- **Description:** This analysis identifies series with the highest-rated pilot episodes (Season 1, Episode 1). A high rating for the very first episode is a strong indicator of a show's ability to immediately capture audience interest and 'hook' them for the rest of the series.

### 7. Sophomore Slump
- **Function:** `sophomore_slump`
- **Business Question:** Which TV series suffer from a 'sophomore slump,' meaning their second season has a noticeable drop in ratings compared to their first?
- **Description:** This analysis calculates the average rating for Season 1 and Season 2 for all series that have at least two seasons. It then highlights the shows with the largest negative rating difference (Season 2 Rating - Season 1 Rating), identifying those that failed to meet audience expectations set by the first season.
---

## Actor Analysis
**File:** `analysis/actor_analysis.py`

### 1. Actor Demographics and Statistics
- **Function:** `actors_demography_stats`
- **Business Question:** What is the profile of the most active and successful actors?
- **Description:** Calculates demographic and career statistics for actors, including their age, the number of titles they have appeared in, and their average film rating.

### 2. Average Rating by Actor
- **Function:** `avg_rating_by_actor`
- **Business Question:** For each actor who appeared in at least 10 films, what is the average rating of those films?
- **Description:** Identifies actors whose participation is statistically associated with high film ratings. This information is valuable for casting directors, producers, and directors when choosing actors for new projects, as well as for actors' agents for positioning their clients.

### 3. Young Actors Age Distribution
- **Function:** `young_actors_2000s`
- **Business Question:** What is the distribution of actors by age groups in films from the last 5 years?
- **Description:** Identifies which age groups of actors are most in demand in modern cinema. This helps casting directors and producers understand trends, studios plan projects for target audiences, and agents position their clients. Age groups: 18-25 (Youth), 26-35 (Prime), 36-45 (Mature), 46-55 (Experienced), 56+ (Veterans).

### 4. Rising Stars
- **Function:** `rising_stars`
- **Business Question:** Which actors are showing the fastest growth in audience demand, making them 'rising stars'?
- **Description:** This analysis identifies the 10 actors with the highest average 'velocity' (rate of change) in the growth of average `numVotes` for their films over the last 5 years. It calculates the year-over-year change in average votes for each actor and then averages this growth rate.

### 5. Fading Stars
- **Function:** `fading_stars`
- **Business Question:** Which established actors are showing a consistent decline in audience demand?
- **Description:** This analysis identifies actors with the most significant *negative* 'velocity' (rate of change) in their average `numVotes` per film over the last 5 years. It pinpoints established actors whose popularity or project engagement may be waning.


## Market Analysis
**File:** `analysis/market_analysis.py` (Assumption)

### 1. Localization Gaps
- **Function:** `localization_gaps`
- **Business Question:** Where are the biggest missed opportunities for localization? Which popular films are not available in key international markets?
- **Description:** This analysis uses the `title.akas` dataset. It first identifies highly popular films (based on `numVotes` or `rating`). It then checks which of these films are *missing* translated titles for specific, high-value regions (e.g., 'DE' for Germany, 'FR' for France, 'ES' for Spain). This reveals gaps where a popular asset could be monetized further with simple localization.