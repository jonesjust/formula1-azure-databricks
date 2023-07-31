# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(
    fields=[
        StructField('circuitId', IntegerType(), False),
        StructField('circuitRef', StringType(), False),
        StructField('name', StringType(), False),
        StructField('location', StringType(), True),
        StructField('country', StringType(), True),
        StructField('lat', DoubleType(), True),
        StructField('lng', DoubleType(), True),
        StructField('alt', IntegerType(), True),
        StructField('url', StringType(), False)
    ]
)

# COMMAND ----------

circuits_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv(f'{raw_container_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt')
)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

circuits_with_data_source_df = circuits_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

circuits_with_file_date_df = circuits_with_data_source_df.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_with_file_date_df)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

display(spark.read.parquet(f'{processed_container_path}/circuits'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit('Success')
