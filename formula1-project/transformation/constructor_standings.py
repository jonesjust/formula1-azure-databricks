# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_container_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

constructor_standings_df = race_results_df \
    .groupBy('race_year', 'team') \
    .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_container_path}/constructor_standings')
