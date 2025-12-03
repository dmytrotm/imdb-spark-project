from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType, StringType
from pyspark.sql import functions as F
from utils.reader import read_data

def count_nulls_nans(df, col):
    # Nulls
    nulls = df.filter(F.col(col).isNull()).count()
    # NaNs (numeric columns can have NaN)
    nans = df.filter(F.isnan(F.col(col))).count() if dict(df.dtypes)[col] in ['float', 'double'] else 0
    return nulls, nans

def detailed_numeric_stats(df, numeric_cols):
    print("\nExtended statistics for numeric columns:")
    for col in numeric_cols:
        stats = df.agg(
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.mean(col).alias("mean"),
            F.stddev(col).alias("stddev"),
            F.countDistinct(col).alias("unique")
        ).collect()[0]

        nulls, nans = count_nulls_nans(df, col)

        print(f"\nColumn: {col}")
        print(f"  Min: {stats['min']}")
        print(f"  Max: {stats['max']}")
        print(f"  Mean: {stats['mean']}")
        print(f"  Stddev: {stats['stddev']}")
        print(f"  Unique values: {stats['unique']}")
        print(f"  Null: {nulls}, NaN: {nans}")

def analyze_string_columns(df, string_cols):
    print("\nAnalysis of text (string) columns:")
    for col in string_cols:
        nulls, _ = count_nulls_nans(df, col)
        unique_count = df.select(col).distinct().count()
        print(f"\nColumn: {col}")
        print(f"  Unique values: {unique_count}")
        print(f"  Null: {nulls}")
        if unique_count < 20:
            print(f"  Values: {[row[0] for row in df.select(col).distinct().collect()]}")

def describe_dataframe(df, name):
    print(f"\n==== TABLE DESCRIPTION: {name} ====")
    df.printSchema()
    print(f"Number of columns: {len(df.columns)}")
    row_count = df.count()
    print(f"Number of rows: {row_count}")
    print(f"First 5 rows:")
    df.show(5, truncate=False)

    print("\nDetailed description of columns (types):")
    for col, dtype in df.dtypes:
        print(f" - {col}: {dtype}")

    numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, NumericType)]
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

    if numeric_cols:
        print(f"\nStatistics for numeric columns (.describe()):")
        stats_df = df.select(numeric_cols).describe()
        stats_df.show()
        detailed_numeric_stats(df, numeric_cols)
    else:
        print("The table has no numeric columns for statistics.")

    if string_cols:
        analyze_string_columns(df, string_cols)
    else:
        print("The table has no text columns for analysis.")

def get_top_regions(akas_df, top_n=5):
    """
    Identifies the top N regions based on the number of DISTINCT titles.
    This prevents regions with many alternate titles from being overrepresented.
    """
    region_counts = (
        akas_df.filter(F.col("region") != "\\N")
        .groupBy("region")
        .agg(F.approx_count_distinct("titleId").alias("distinct_titles"))
        .orderBy(F.col("distinct_titles").desc())
        .limit(top_n)
    )
    return [row.region for row in region_counts.collect()]

def filter_by_region(df, akas_df, top_n=5, tconst_col="tconst"):
    """
    Filters a DataFrame to include only titles present in the top N regions.
    """
    top_regions = get_top_regions(akas_df, top_n)
    print(f"Filtering by top {top_n} regions: {', '.join(top_regions)}")
    
    # Get titles available in these regions
    valid_titles = akas_df.filter(F.col("region").isin(top_regions)).select(F.col("titleId").alias(tconst_col)).distinct()
    
    # Broadcast join for efficiency if valid_titles is small, but it might not be.
    # Standard join is safer for large datasets.
    return df.join(valid_titles, tconst_col, "inner")