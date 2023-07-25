# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('year', IntegerType(), False),
        StructField('round', IntegerType(), False),
        StructField('circuitId', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('date', DateType(), False),
        StructField('time', StringType(), True),
        StructField('url', StringType(), True)
    ]
)

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv(f'{raw_container_path}/races.csv')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(
    col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('date'), col('time')
)

# COMMAND ----------

races_renamed_df = races_selected_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

# COMMAND ----------

races_with_timestamp_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_data_source_df = races_with_timestamp_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_data_source_df)

# COMMAND ----------

races_final_df = races_with_ingestion_date_df.select(
    col('race_id'), col('race_year'), col('round'), col('circuit_id'), col('name'), col('race_timestamp'), col('data_source'), col('ingestion_date')
)

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_container_path}/races')

# COMMAND ----------

display(spark.read.parquet(f'{processed_container_path}/races'))

# COMMAND ----------

dbutils.notebook.exit('Success')
