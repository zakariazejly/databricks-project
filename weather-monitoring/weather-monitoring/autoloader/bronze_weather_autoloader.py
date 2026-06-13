# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/weather_lakehouse/raw/checkpoints/schema/") 
    .load("/Volumes/weather_lakehouse/raw/weather_api_raw/")
)

df.writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/weather_lakehouse/raw/checkpoints/bronze/")\
    .trigger(once=True)\
    .outputMode("append")\
    .table("weather_lakehouse.bronze.weather_api_bronze")