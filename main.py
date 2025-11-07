
from pyspark.sql import SparkSession
from reader import read_data

def main():
    spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()

    dataframes = read_data(spark)

    ratings_df = dataframes["title.ratings"]
    print("Schema for title.ratings:")
    ratings_df.printSchema()
    print("Top 5 rows from title.ratings:")
    ratings_df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
