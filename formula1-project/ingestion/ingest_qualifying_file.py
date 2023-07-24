# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(
    fields=[
        StructField('qualifyId', IntegerType(), False),
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('constructorId', IntegerType(), False),
        StructField('number', IntegerType(), False),
        StructField('position', IntegerType(), True),
        StructField('q1', StringType(), True),
        StructField('q2', StringType(), True),
        StructField('q3', StringType(), True)
    ]
)

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option('multiline', True) \
    .json('/mnt/formula1adls22/raw/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df \
    .withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet('/mnt/formula1adls22/processed/qualifying')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/qualifying'))
