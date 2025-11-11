"""
Analysis results visualization module
"""
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
from datetime import datetime
import os

# Settings for running without display (in Docker)
matplotlib.use('Agg')

# Create directory for saving charts
OUTPUT_DIR = "/app/visualization"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_plot(fig, filename, title):
    """Save chart to file"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"✅ Chart saved: {filepath}")
    return filepath


def plot_top_genres(spark_df):
    """
    Visualization of top-5 genres by engagement after 2010
    
    Args:
        spark_df: Spark DataFrame with columns (genre, film_count, avg_rating, 
                  total_votes, avg_votes_per_film, engagement_score)
    """
    # Convert Spark DataFrame to Pandas
    pdf = spark_df.toPandas()
    
    # Create figure with three subplots
    fig = plt.figure(figsize=(18, 6))
    gs = fig.add_gridspec(1, 3, hspace=0.3, wspace=0.3)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[0, 2])
    
    # Chart 1: Engagement Score
    colors1 = plt.cm.RdYlGn(pdf['engagement_score'] / pdf['engagement_score'].max())
    bars1 = ax1.barh(range(len(pdf)), pdf['engagement_score'], color=colors1, edgecolor='black')
    ax1.set_yticks(range(len(pdf)))
    ax1.set_yticklabels(pdf['genre'], fontsize=10)
    ax1.set_xlabel('Engagement Score', fontsize=11, fontweight='bold')
    ax1.set_title('Engagement Score\n(Popularity × Quality)', fontsize=11, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    for i, (bar, score) in enumerate(zip(bars1, pdf['engagement_score'])):
        ax1.text(score, bar.get_y() + bar.get_height()/2, 
                f' {score:.1f}', 
                va='center', fontsize=9, fontweight='bold')
    
    # Chart 2: Average rating vs Film count
    scatter = ax2.scatter(pdf['film_count'], pdf['avg_rating'], 
                         s=pdf['avg_votes_per_film']/50, 
                         c=pdf['engagement_score'], 
                         cmap='RdYlGn', alpha=0.7, edgecolors='black', linewidth=2)
    
    for i, genre in enumerate(pdf['genre']):
        ax2.annotate(genre, 
                    (pdf['film_count'].iloc[i], pdf['avg_rating'].iloc[i]),
                    fontsize=9, fontweight='bold',
                    xytext=(5, 5), textcoords='offset points')
    
    ax2.set_xlabel('Film count', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Average rating', fontsize=11, fontweight='bold')
    ax2.set_title('Rating vs Count\n(size = votes/film)', fontsize=11, fontweight='bold')
    ax2.grid(alpha=0.3)
    
    # Chart 3: Average votes per film
    bars3 = ax3.barh(range(len(pdf)), pdf['avg_votes_per_film'], color='#e74c3c', alpha=0.8)
    ax3.set_yticks(range(len(pdf)))
    ax3.set_yticklabels(pdf['genre'], fontsize=10)
    ax3.set_xlabel('Avg votes/film', fontsize=11, fontweight='bold')
    ax3.set_title('Film popularity\nby genre', fontsize=11, fontweight='bold')
    ax3.grid(axis='x', alpha=0.3)
    
    for bar, votes in zip(bars3, pdf['avg_votes_per_film']):
        ax3.text(votes, bar.get_y() + bar.get_height()/2, 
                f' {int(votes):,}', 
                va='center', fontsize=9, fontweight='bold')
    
    plt.suptitle('Top-5 Genres by Engagement After 2010', 
                 fontsize=14, fontweight='bold', y=0.98)
    
    return save_plot(fig, 'top_genres_after_2010.png', 'Top Genres')


def plot_series_ratings(spark_df):
    """
    Visualization of top-10 series with 50+ episodes
    
    Args:
        spark_df: Spark DataFrame with columns (primaryTitle, episode_count, averageRating)
    """
    # Convert to Pandas and take top-10
    pdf = spark_df.orderBy('averageRating', ascending=False).limit(10).toPandas()
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Chart 1: Ratings
    bars1 = ax1.barh(range(len(pdf)), pdf['averageRating'], color='#2ecc71')
    ax1.set_yticks(range(len(pdf)))
    ax1.set_yticklabels(pdf['primaryTitle'], fontsize=9)
    ax1.set_xlabel('Average rating', fontsize=11, fontweight='bold')
    ax1.set_title('Top-10 series by rating\n(50+ episodes)', 
                  fontsize=12, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # Add values on bars
    for i, (bar, rating) in enumerate(zip(bars1, pdf['averageRating'])):
        ax1.text(rating, bar.get_y() + bar.get_height()/2, 
                f' {rating:.2f}', 
                va='center', fontsize=9, fontweight='bold')
    
    # Chart 2: Episode count
    bars2 = ax2.barh(range(len(pdf)), pdf['episode_count'], color='#e74c3c')
    ax2.set_yticks(range(len(pdf)))
    ax2.set_yticklabels(pdf['primaryTitle'], fontsize=9)
    ax2.set_xlabel('Episode count', fontsize=11, fontweight='bold')
    ax2.set_title('Number of episodes', fontsize=12, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # Add values
    for i, (bar, count) in enumerate(zip(bars2, pdf['episode_count'])):
        ax2.text(count, bar.get_y() + bar.get_height()/2, 
                f' {int(count)}', 
                va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    return save_plot(fig, 'long_series_ratings.png', 'Series Ratings')


def plot_season_trends(spark_df):
    """
    Visualization of season rating trends
    
    Args:
        spark_df: Spark DataFrame with columns (primaryTitle, seasonNumber, rating_diff)
    """
    # Top-5 series with biggest rating increases
    top_increases = spark_df.filter("rating_diff IS NOT NULL") \
        .orderBy('rating_diff', ascending=False).limit(5).toPandas()
    
    # Top-5 series with biggest rating decreases
    top_decreases = spark_df.filter("rating_diff IS NOT NULL") \
        .orderBy('rating_diff', ascending=True).limit(5).toPandas()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Increase chart
    bars1 = ax1.barh(range(len(top_increases)), top_increases['rating_diff'], color='#27ae60')
    ax1.set_yticks(range(len(top_increases)))
    labels1 = [f"{row['primaryTitle']}\n(Season {row['seasonNumber']})" 
               for _, row in top_increases.iterrows()]
    ax1.set_yticklabels(labels1, fontsize=8)
    ax1.set_xlabel('Rating change', fontsize=11, fontweight='bold')
    ax1.set_title('Top-5 biggest rating increases\nbetween seasons', 
                  fontsize=12, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    for bar, diff in zip(bars1, top_increases['rating_diff']):
        ax1.text(diff, bar.get_y() + bar.get_height()/2, 
                f' +{diff:.2f}', 
                va='center', fontsize=9, fontweight='bold')
    
    # Decrease chart
    bars2 = ax2.barh(range(len(top_decreases)), top_decreases['rating_diff'], color='#c0392b')
    ax2.set_yticks(range(len(top_decreases)))
    labels2 = [f"{row['primaryTitle']}\n(Season {row['seasonNumber']})" 
               for _, row in top_decreases.iterrows()]
    ax2.set_yticklabels(labels2, fontsize=8)
    ax2.set_xlabel('Rating change', fontsize=11, fontweight='bold')
    ax2.set_title('Top-5 biggest rating drops\nbetween seasons', 
                  fontsize=12, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    for bar, diff in zip(bars2, top_decreases['rating_diff']):
        ax2.text(diff, bar.get_y() + bar.get_height()/2, 
                f' {diff:.2f}', 
                va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    return save_plot(fig, 'season_rating_trends.png', 'Season Trends')


def plot_actor_ratings(spark_df):
    """
    Visualization of top-20 actors by average rating
    
    Args:
        spark_df: Spark DataFrame with columns (primaryName, film_count, avg_rating)
    """
    # Top-20 actors
    pdf = spark_df.orderBy('avg_rating', ascending=False).limit(20).toPandas()
    
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Create color palette from dark green to light green
    colors = plt.cm.RdYlGn(pdf['avg_rating'] / pdf['avg_rating'].max())
    
    bars = ax.barh(range(len(pdf)), pdf['avg_rating'], color=colors)
    ax.set_yticks(range(len(pdf)))
    
    # Add name and film count
    labels = [f"{row['primaryName']} ({int(row['film_count'])} films)" 
              for _, row in pdf.iterrows()]
    ax.set_yticklabels(labels, fontsize=9)
    
    ax.set_xlabel('Average film rating', fontsize=11, fontweight='bold')
    ax.set_title('Top-20 actors with highest average film rating\n(minimum 10 films)', 
                 fontsize=13, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    ax.set_xlim(pdf['avg_rating'].min() - 0.2, pdf['avg_rating'].max() + 0.2)
    
    # Add values
    for bar, rating in zip(bars, pdf['avg_rating']):
        ax.text(rating, bar.get_y() + bar.get_height()/2, 
                f' {rating:.2f}', 
                va='center', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    return save_plot(fig, 'top_actors_by_rating.png', 'Actor Ratings')


def plot_young_actors_distribution(spark_df):
    """
    Visualization of actor distribution by age groups in recent films
    
    Args:
        spark_df: Spark DataFrame with columns (age_group, unique_actors, total_roles, avg_age)
    """
    # Convert to Pandas
    pdf = spark_df.toPandas()
    
    # Reorder for better visualization
    age_order = ["18-25 (Youth)", "26-35 (Prime)", "36-45 (Mature)", 
                 "46-55 (Experienced)", "56+ (Veterans)"]
    pdf['age_group'] = pd.Categorical(pdf['age_group'], categories=age_order, ordered=True)
    pdf = pdf.sort_values('age_group')
    
    fig = plt.figure(figsize=(18, 6))
    gs = fig.add_gridspec(1, 3, hspace=0.3, wspace=0.3)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[0, 2])
    
    # Color palette from green to blue
    colors = ['#2ecc71', '#3498db', '#9b59b6', '#e67e22', '#e74c3c']
    
    # Chart 1: Unique actors count
    bars1 = ax1.bar(range(len(pdf)), pdf['unique_actors'], color=colors, 
                    alpha=0.8, edgecolor='black', linewidth=2)
    ax1.set_xticks(range(len(pdf)))
    ax1.set_xticklabels([ag.split(' ')[0] for ag in pdf['age_group']], 
                        fontsize=10, fontweight='bold')
    ax1.set_ylabel('Number of unique actors', fontsize=11, fontweight='bold')
    ax1.set_title('Unique actors\nby age group', fontsize=11, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    for bar, count in zip(bars1, pdf['unique_actors']):
        ax1.text(bar.get_x() + bar.get_width()/2, count, 
                f'{int(count):,}', 
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Chart 2: Total roles count
    bars2 = ax2.bar(range(len(pdf)), pdf['total_roles'], color=colors, 
                    alpha=0.8, edgecolor='black', linewidth=2)
    ax2.set_xticks(range(len(pdf)))
    ax2.set_xticklabels([ag.split(' ')[0] for ag in pdf['age_group']], 
                        fontsize=10, fontweight='bold')
    ax2.set_ylabel('Total number of roles', fontsize=11, fontweight='bold')
    ax2.set_title('Total roles\nin films 2019-2024', fontsize=11, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    for bar, count in zip(bars2, pdf['total_roles']):
        ax2.text(bar.get_x() + bar.get_width()/2, count, 
                f'{int(count):,}', 
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Chart 3: Pie chart of role distribution
    wedges, texts, autotexts = ax3.pie(pdf['total_roles'], 
                                        labels=[ag.split(' ')[1] for ag in pdf['age_group']], 
                                        autopct='%1.1f%%',
                                        colors=colors,
                                        startangle=90,
                                        explode=[0.05] * len(pdf))
    ax3.set_title('% role distribution\nby age group', fontsize=11, fontweight='bold')
    
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(10)
    
    for text in texts:
        text.set_fontsize(10)
        text.set_fontweight('bold')
    
    plt.suptitle('Actor Distribution by Age Groups in Films 2019-2024', 
                 fontsize=14, fontweight='bold', y=0.98)
    
    return save_plot(fig, 'young_actors_distribution.png', 'Age Distribution')


def plot_director_stats(spark_df, directors_films_df=None):
    """
    Visualization of top-3 directors
    
    Args:
        spark_df: Spark DataFrame with columns (primaryName, high_rated_count, avg_rating)
        directors_films_df: Optional DataFrame with directors' films
    """
    pdf = spark_df.toPandas()
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Chart 1: High-rated films count
    bars1 = ax1.bar(range(len(pdf)), pdf['high_rated_count'], 
                    color=['#f39c12', '#e67e22', '#d35400'], 
                    edgecolor='black', linewidth=2)
    ax1.set_xticks(range(len(pdf)))
    ax1.set_xticklabels(pdf['primaryName'], fontsize=11, fontweight='bold')
    ax1.set_ylabel('Number of films (rating > 8.0)', fontsize=11, fontweight='bold')
    ax1.set_title('High-rated films count', 
                  fontsize=12, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # Add values
    for bar, count in zip(bars1, pdf['high_rated_count']):
        ax1.text(bar.get_x() + bar.get_width()/2, count, 
                f'{int(count)}', 
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Add medals
    medals = ['🥇', '🥈', '🥉']
    for i, (bar, medal) in enumerate(zip(bars1, medals)):
        ax1.text(bar.get_x() + bar.get_width()/2, 
                bar.get_height() * 0.5, 
                medal, 
                ha='center', va='center', fontsize=30)
    
    # Chart 2: Average rating
    bars2 = ax2.bar(range(len(pdf)), pdf['avg_rating'], 
                    color=['#f39c12', '#e67e22', '#d35400'], 
                    edgecolor='black', linewidth=2)
    ax2.set_xticks(range(len(pdf)))
    ax2.set_xticklabels(pdf['primaryName'], fontsize=11, fontweight='bold')
    ax2.set_ylabel('Average rating', fontsize=11, fontweight='bold')
    ax2.set_title('Average rating of high-rated films', 
                  fontsize=12, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    ax2.set_ylim(pdf['avg_rating'].min() - 0.2, pdf['avg_rating'].max() + 0.2)
    
    # Add values
    for bar, rating in zip(bars2, pdf['avg_rating']):
        ax2.text(bar.get_x() + bar.get_width()/2, rating, 
                f'{rating:.2f}', 
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    plt.suptitle('Top-3 Directors with Most High-Rated Films\n(rating > 8.0, runtime > 90 min)', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    
    return save_plot(fig, 'top_directors.png', 'Top Directors')

