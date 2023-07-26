# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_container_path}/circuits') \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_container_path}/races') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_container_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_container_path}/drivers') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_container_path}/results') \
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df \
    .join(races_circuits_df, results_df.race_id == races_circuits_df.race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df \
    .select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points') \
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df.filter('race_year == 2020 and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{presentation_container_path}/race_results')
