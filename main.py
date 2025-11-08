from pyspark.sql import SparkSession
from utils.reader import read_data
from utils.analysis import describe_dataframe

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    # Read the data
    dataframes = read_data(spark)

    # Analyze each dataframe
    for name, df in dataframes.items():
        describe_dataframe(df, name)

    spark.stop()

if __name__ == "__main__":
    main()