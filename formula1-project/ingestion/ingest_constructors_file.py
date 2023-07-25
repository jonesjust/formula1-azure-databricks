# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

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

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_with_data_source_df = constructors_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_with_data_source_df)

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/constructors')

# COMMAND ----------

display(spark.read.parquet(f'{processed_container_path}/constructors'))

# COMMAND ----------

dbutils.notebook.exit('Success')
