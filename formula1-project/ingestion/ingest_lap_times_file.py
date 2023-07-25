# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('lap', IntegerType(), False),
        StructField('position', StringType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_container_path}/lap_times')

# COMMAND ----------

lap_times_renamed_df = lap_times_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/lap_times')
