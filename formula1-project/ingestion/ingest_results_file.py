# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(
    fields=[
        StructField('resultId', IntegerType(), False),
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('constructorId', IntegerType(), False),
        StructField('number', IntegerType(), True),
        StructField('grid', IntegerType(), False),
        StructField('position', IntegerType(), True),
        StructField('positionText', StringType(), False),
        StructField('positionOrder', IntegerType(), False),
        StructField('points', FloatType(), False),
        StructField('laps', IntegerType(), False),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True),
        StructField('fastestLap', IntegerType(), True),
        StructField('rank', IntegerType(), True),
        StructField('fastestLapTime', StringType(), True),
        StructField('fastestLapSpeed', StringType(), True),
        StructField('statusId', IntegerType(), False),
    ]
)

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json('/mnt/formula1adls22/raw/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_renamed_df = results_df \
    .withColumnRenamed('resultId', 'result_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'position_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLap', 'fastest_lap') \
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

results_drop_statusid_df = results_renamed_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_final_df = results_drop_statusid_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formula1adls22/processed/results')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/results'))