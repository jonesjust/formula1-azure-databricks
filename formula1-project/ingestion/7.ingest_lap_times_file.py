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

lap_times_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('lap', IntegerType(), False),
        StructField('position', StringType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]
)

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_container_path}/{v_file_date}/lap_times')

# COMMAND ----------

lap_times_renamed_df = lap_times_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_with_data_source_df = lap_times_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

lap_times_with_file_date_df = lap_times_with_data_source_df.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_with_file_date_df)

# COMMAND ----------

overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
