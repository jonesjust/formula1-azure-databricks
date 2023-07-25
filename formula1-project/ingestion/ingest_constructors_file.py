# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

# COMMAND ----------

constructors_schema = StructType(
    fields=[
        StructField('constructorId', IntegerType(), False),
        StructField('constructorRef', StringType(), False),
        StructField('name', StringType(), False),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), False)
    ]
)

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f'{raw_container_path}/constructors.json')

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref')   

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/constructors')
