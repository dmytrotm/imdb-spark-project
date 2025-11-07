
from pyspark.sql import SparkSession
from schemas import (
    name_basics_schema, title_akas_schema, title_basics_schema,
    title_crew_schema, title_episode_schema, title_principals_schema,
    title_ratings_schema
)


def read_data(spark: SparkSession):
    data_sources = {
        "name.basics": ("/data/name.basics.tsv", name_basics_schema),
        "title.akas": ("/data/title.akas.tsv", title_akas_schema),
        "title.basics": ("/data/title.basics.tsv", title_basics_schema),
        "title.crew": ("/data/title.crew.tsv", title_crew_schema),
        "title.episode": ("/data/title.episode.tsv", title_episode_schema),
        "title.principals": ("/data/title.principals.tsv", title_principals_schema),
        "title.ratings": ("/data/title.ratings.tsv", title_ratings_schema),
    }

    dataframes = {}
    for name, (path, schema) in data_sources.items():
        df = spark.read.csv(
            path, 
            schema=schema, 
            header=True, 
            sep='\t'
        )
        dataframes[name] = df

    return dataframes
