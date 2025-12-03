from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import os


def evaluate_regression_models(predictions_dict, label_col="label"):
    """Evaluate multiple regression models and return metrics"""
    evaluator_rmse = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="r2"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName="mae"
    )

    results = []
    for model_name, predictions in predictions_dict.items():
        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        results.append({"Model": model_name, "RMSE": rmse, "R2": r2, "MAE": mae})
    return results


def evaluate_classification_models(predictions_dict, label_col="label"):
    """Evaluate multiple classification models and return metrics"""
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol=label_col, metricName="areaUnderROC"
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol=label_col, metricName="areaUnderPR"
    )

    results = []
    for model_name, predictions in predictions_dict.items():
        auc = evaluator_auc.evaluate(predictions)
        pr = evaluator_pr.evaluate(predictions)

        correct = predictions.filter(F.col(label_col) == F.col("prediction")).count()
        total = predictions.count()
        accuracy = correct / total if total > 0 else 0

        results.append(
            {"Model": model_name, "AUC": auc, "PR-AUC": pr, "Accuracy": accuracy}
        )
    return results


def plot_model_comparison(results, task_name, save_path="."):
    """Plot model comparison metrics"""
    import pandas as pd

    df = pd.DataFrame(results)
    metrics = [col for col in df.columns if col != "Model"]

    fig, axes = plt.subplots(1, len(metrics), figsize=(5 * len(metrics), 5))
    if len(metrics) == 1:
        axes = [axes]

    for i, metric in enumerate(metrics):
        axes[i].barh(
            df["Model"], df[metric], color=sns.color_palette("viridis", len(df))
        )
        axes[i].set_xlabel(metric, fontsize=12, fontweight="bold")
        axes[i].set_title(f"{metric} Comparison", fontsize=13, fontweight="bold")
        axes[i].grid(True, alpha=0.3, axis="x")

        for j, v in enumerate(df[metric]):
            axes[i].text(v + 0.01, j, f"{v:.4f}", va="center", fontsize=10)

    plt.suptitle(
        f"{task_name} - Model Comparison", fontsize=15, fontweight="bold", y=1.02
    )
    plt.tight_layout()
    plt.savefig(
        os.path.join(
            save_path, f"{task_name.lower().replace(' ', '_')}_comparison.png"
        ),
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()