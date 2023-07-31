# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

race_results_list = spark.read.parquet(f'{presentation_container_path}/race_results') \
    .filter(f"file_date = '{v_file_date}'") \
    .select('race_year') \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f'{presentation_container_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

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

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;
