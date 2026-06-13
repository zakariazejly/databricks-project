# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, current_date, hour, col

# COMMAND ----------

df = spark.sql("""
SELECT city, hour, temperature
FROM weather_lakehouse.silver.weather_silver
WHERE city = 'fes'
  AND to_date(to_timestamp(hour)) = current_date()
ORDER BY hour DESC
LIMIT 100
""")
display(df)

# COMMAND ----------

df2 = spark.sql("""
SELECT city, hour, temperature
FROM weather_lakehouse.silver.weather_silver
WHERE city = 'fes'
ORDER BY hour DESC
LIMIT 100
""")
df_weather2 = (
    df2.withColumn("hour_ts", to_date(col("hour")))
        .filter(col("hour_ts") == current_date()
))
display(df_weather2)


# COMMAND ----------

from pyspark.sql.functions import current_date

# Add a new column named "today"
df_with_date = df_with_date.withColumn("today", current_date())
df_with_date.show()