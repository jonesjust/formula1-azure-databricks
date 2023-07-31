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

pit_stops_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('stop', IntegerType(), False),
        StructField('lap', IntegerType(), False),
        StructField('time', StringType(), False),
        StructField('duration', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option('multiline', True) \
    .json(f'{raw_container_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_with_data_source_df = pit_stops_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

pit_stops_with_file_date_df = pit_stops_with_data_source_df.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_with_file_date_df)

# COMMAND ----------

overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
