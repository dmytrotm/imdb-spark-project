from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType, StringType
from pyspark.sql import functions as F
from utils.reader import read_data

def count_nulls_nans(df, col):
    # Nulls
    nulls = df.filter(F.col(col).isNull()).count()
    # NaNs (числові колонки можуть мати NaN)
    nans = df.filter(F.isnan(F.col(col))).count() if dict(df.dtypes)[col] in ['float', 'double'] else 0
    return nulls, nans

def detailed_numeric_stats(df, numeric_cols):
    print("\nРозширена статистика по числових колонках:")
    for col in numeric_cols:
        stats = df.agg(
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.mean(col).alias("mean"),
            F.stddev(col).alias("stddev"),
            F.countDistinct(col).alias("unique")
        ).collect()[0]

        nulls, nans = count_nulls_nans(df, col)

        print(f"\nКолонка: {col}")
        print(f"  Мінімум: {stats['min']}")
        print(f"  Максимум: {stats['max']}")
        print(f"  Середнє: {stats['mean']}")
        print(f"  Stddev: {stats['stddev']}")
        print(f"  Унікальних значень: {stats['unique']}")
        print(f"  Null: {nulls}, NaN: {nans}")

def analyze_string_columns(df, string_cols):
    print("\nАналіз текстових колонок (строкових):")
    for col in string_cols:
        nulls, _ = count_nulls_nans(df, col)
        unique_count = df.select(col).distinct().count()
        print(f"\nКолонка: {col}")
        print(f"  Унікальних значень: {unique_count}")
        print(f"  Null: {nulls}")
        if unique_count < 20:
            print(f"  Значення: {[row[0] for row in df.select(col).distinct().collect()]}")

def describe_dataframe(df, name):
    print(f"\n==== ОПИС ТАБЛИЦІ: {name} ====")
    df.printSchema()
    print(f"Кількість стовпців: {len(df.columns)}")
    row_count = df.count()
    print(f"Кількість рядків: {row_count}")
    print(f"Перші 5 рядків:")
    df.show(5, truncate=False)

    print("\nДетальний опис колонок (типи):")
    for col, dtype in df.dtypes:
        print(f" - {col}: {dtype}")

    numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

    if numeric_cols:
        print(f"\nСтатистика по числових стовпцях (.describe()):")
        stats_df = df.select(numeric_cols).describe()
        stats_df.show()
        detailed_numeric_stats(df, numeric_cols)
    else:
        print("У таблиці немає числових колонок для статистики.")

    if string_cols:
        analyze_string_columns(df, string_cols)
    else:
        print("У таблиці немає текстових колонок для аналізу.")

def main():
    spark = SparkSession.builder.appName("IMDB Dataset Analysis").getOrCreate()
    datasets = read_data(spark)

    for name, df in datasets.items():
        describe_dataframe(df, name)

    spark.stop()

if __name__ == "__main__":
    main()
