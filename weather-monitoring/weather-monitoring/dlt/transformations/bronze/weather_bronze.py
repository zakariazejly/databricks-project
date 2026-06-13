import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze.weather_bronze",
    comment="Bronze table containing raw weather API data for all cities."
)
def weather_bronze():
    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "json")\
        .load("/Volumes/weather_lakehouse/raw/weather_api_raw")\
        .withColumn("source_file", col("_metadata.file_path"))\
        .withColumn("ingestion_time", current_timestamp())
    return df