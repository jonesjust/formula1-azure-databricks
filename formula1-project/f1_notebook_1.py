# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-sscope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://test@formula1adls22.dfs.core.windows.net/",
  mount_point = "/mnt/formula1adls22/test",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1adls22/test'))

# COMMAND ----------

display(spark.read.csv('/mnt/formula1adls22/test/circuits.csv'))
