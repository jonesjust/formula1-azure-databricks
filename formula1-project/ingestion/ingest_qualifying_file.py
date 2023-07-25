# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

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
    .json(f'{raw_container_path}/qualifying')

# COMMAND ----------

qualifying_renamed_df = qualifying_df \
    .withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/qualifying')
