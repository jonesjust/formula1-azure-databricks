# Databricks notebook source
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
    .csv('/mnt/formula1adls22/raw/lap_times')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet('/mnt/formula1adls22/processed/lap_times')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/lap_times'))
