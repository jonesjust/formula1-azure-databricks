# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

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
    .json(f'{raw_container_path}/{v_file_date}/qualifying')

# COMMAND ----------

qualifying_renamed_df = qualifying_df \
    .withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_with_data_source_df = qualifying_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

qualifying_with_file_date_df = qualifying_with_data_source_df.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_with_file_date_df)

# COMMAND ----------

overwrite_partition(qualifying_final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

display(spark.read.parquet(f'{processed_container_path}/qualifying'))

# COMMAND ----------

dbutils.notebook.exit('Success')
