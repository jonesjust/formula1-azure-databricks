# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adls22/raw

# COMMAND ----------

circuits_df = spark.read.option('header', True).csv('/mnt/formula1adls22/raw/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


