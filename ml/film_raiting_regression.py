from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from .metrics import calculate_regression_metrics

def film_raiting_linear(spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,):
    model =  LinearRegression(
        featuresCol="features",
        labelCol="label",
        regParam=0.01
    )
    
    print("Алгоритм: Linear Regressor. Запуск навчання...")
    return train_model_for_film_raiting_regression(spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,model)

def film_raiting_decision_tree(spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,):
    model =  DecisionTreeRegressor(
        featuresCol="features",
        labelCol="label",
        maxDepth=5,     
        maxBins=32
    )
    print("Алгоритм: Decision Tree Regressor. Запуск навчання...")
    return train_model_for_film_raiting_regression(spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,model)

def film_raiting_random_forest(spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,):
    
    
    model =  RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=30,      
        maxDepth=8,       
        seed=42
    )
    
    print("Алгоритм: Random Forest Regressor. Запуск навчання...")
    
    return train_model_for_film_raiting_regression(
        spark,
        dataframes,
        actors_stats_df,     
        rising_stars_df,
        model
    )

def train_model_for_film_raiting_regression(
    spark,
    dataframes,
    actors_stats_df,     
    rising_stars_df,
    model      
):
    """
    Прогнозує рейтинг фільму на основі агрегованих метрик акторського капіталу.
    Узгоджено з оновленими функціями: avg_rating_by_actor(), rising_stars().
    """

    principals_df = dataframes['title.principals']
    ratings_df = dataframes['title.ratings']

    actors_stats_clean = actors_stats_df.select(
        "nconst",
        F.col("avg_rating"),
        F.col("film_count").alias("num_titles")    
    )

    rising_clean = rising_stars_df.select(
        "nconst",
        "avg_velocity"
    )

    actor_metrics = actors_stats_clean.join(rising_clean, "nconst", "left") \
                                      .fillna(0, subset=['avg_velocity'])

   
    top_principals = principals_df.filter(
        F.col("category").isin(["actor", "actress"])
    ).filter(
        F.col("ordering").between(1, 3)
    ).select("tconst", "nconst", "ordering")

    
    film_actor_metrics = top_principals.join(actor_metrics, "nconst", "inner")

    movie_features = film_actor_metrics.groupBy("tconst").agg(

        # Усереднена зважена якість
        (F.sum(F.col("avg_rating") * F.col("num_titles")) /
         F.sum("num_titles")).alias("ActorCapital_Quality"),

        # Середня швидкість росту популярності акторів
        F.avg("avg_velocity").alias("ActorCapital_Velocity"),

        # Загальна "вага" акторського досвіду
        F.sum("num_titles").alias("ActorCapital_TotalCareerVolume")
    )

   

    final_data = ratings_df.select(
        "tconst",
        F.col("averageRating").alias("label")
    ).join(movie_features, "tconst", "inner") \
     .filter(F.col("label").isNotNull())

    if final_data.count() == 0:
        raise ValueError("❌ Немає даних для тренування регресії — перевір джоіни та фільтри.")

  
    feature_cols = [
        "ActorCapital_Quality",
        "ActorCapital_Velocity",
        "ActorCapital_TotalCareerVolume"
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    lr = model 
    pipeline = Pipeline(stages=[assembler, lr])

   
    train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

    print("Навчання регресійної моделі...")
    model = pipeline.fit(train_data)

    predictions = model.transform(test_data)

    calculate_regression_metrics(predictions)

    return model




def make_prediction_for_new_film(model_pipeline, new_film_features_df):
    """
    Застосовує навчену модель для прогнозування рейтингу.
    """
    prediction_df = model_pipeline.transform(new_film_features_df)
    
    return prediction_df.select(
        "tconst", 
        "prediction"
    ).withColumnRenamed("prediction", "predicted_averageRating")