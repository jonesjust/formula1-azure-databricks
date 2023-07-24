# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(
    fields=[
        StructField('forename', StringType(), False),
        StructField('surname', StringType(), False)
    ]
)

# COMMAND ----------

drivers_schema = StructType(
    fields=[
        StructField('driverId', IntegerType(), False),
        StructField('driverRef', StringType(), False),
        StructField('number', IntegerType(), True),
        StructField('code', StringType(), True),
        StructField('name', name_schema),
        StructField('dob', DateType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), False),
    ]
)

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json('/mnt/formula1adls22/raw/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

drivers_with_columns_df = drivers_df \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef', 'driver_ref') \
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop('url')

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('/mnt/formula1adls22/processed/drivers')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/drivers'))