# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adls22/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('year', IntegerType(), True),
        StructField('round', IntegerType(), True),
        StructField('circuitId', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('date', DateType(), True),
        StructField('time', StringType(), True),
        StructField('url', StringType(), True)
    ]
)

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv('/mnt/formula1adls22/raw/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(
    col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('date'), col('time')
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_renamed_df = races_selected_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

# COMMAND ----------

races_with_timestamp_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_with_ingestion_date_df = races_with_timestamp_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

races_final_df = races_with_ingestion_date_df.select(
    col('race_id'), col('race_year'), col('round'), col('circuit_id'), col('name'), col('race_timestamp'), col('ingestion_date')
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1adls22/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/races'))
