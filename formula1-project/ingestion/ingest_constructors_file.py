# Databricks notebook source
# MAGIC %run ../includes/configuration

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

display(constructors_df)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('ingestion_date', current_timestamp())    

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/constructors')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/constructors'))
