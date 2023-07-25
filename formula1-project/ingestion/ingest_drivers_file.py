# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

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
    .json(f'{raw_container_path}/drivers.json')

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef', 'driver_ref') \
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

drivers_drop_url_df = drivers_with_columns_df.drop('url')

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_drop_url_df)

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/drivers')
