# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

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
    .json(f'{raw_container_path}/pit_stops.json')

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_with_data_source_df = pit_stops_renamed_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_with_data_source_df)

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/pit_stops')

# COMMAND ----------

display(spark.read.parquet(f'{processed_container_path}/pit_stops'))

# COMMAND ----------

dbutils.notebook.exit('Success')
