# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('stop', IntegerType(), False),
        StructField('lap', IntegerType(), False),
        StructField('time', StringType(), False),
        StructField('duration', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option('multiline', True) \
    .json('/mnt/formula1adls22/raw/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet('/mnt/formula1adls22/processed/pit_stops')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/pit_stops'))