# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adls22/raw

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
    .csv(f'{raw_container_path}/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt')
)

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f'{processed_container_path}/circuits')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1adls22/processed/circuits'))
