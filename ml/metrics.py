from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_regression_metrics(
    predictions_df: DataFrame,
    label_col: str = "label",
    prediction_col: str = "prediction"
):
    """
    Універсальна функція для обчислення та друку ключових метрик регресії 
    на основі DataFrame, що містить фактичні ('label') та прогнозовані ('prediction') значення.

    :param predictions_df: PySpark DataFrame з колонками 'label_col' та 'prediction_col'.
    :param label_col: Назва колонки з фактичними значеннями (за замовчуванням 'label').
    :param prediction_col: Назва колонки з прогнозованими значеннями (за замовчуванням 'prediction').
    """
    
    print("-" * 60)
    if predictions_df.count() == 0:
        print("Вхідний DataFrame прогнозів порожній. Неможливо виконати оцінку.")
        return

  
    base_evaluator = RegressionEvaluator(
        labelCol=label_col, 
        predictionCol=prediction_col
    )
    
   
    rmse = base_evaluator.setMetricName("rmse").evaluate(predictions_df)
    print(f"1. RMSE (Середньоквадратична помилка кореня): {rmse:.4f}")

    
    r2 = base_evaluator.setMetricName("r2").evaluate(predictions_df)
    print(f"2. R-squared (Частка поясненої дисперсії): {r2:.4f}")
    
    
    mae = base_evaluator.setMetricName("mae").evaluate(predictions_df)
    print(f"3. MAE (Середня абсолютна помилка): {mae:.4f}")
    
    print("-" * 60)