# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-sscope')

# COMMAND ----------

def mount_adls_container(storage_account_name, container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-sscope', key = 'formula1-app-client-secret')
    
    # set spark configuration
    configs = {
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # unmount the container if it already exists
    if any(mount.mountPoint == f'/mnt/{storage_account_name}/{container_name}' for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f'/mnt/{storage_account_name}/{container_name}')
    
    # mount the container
    dbutils.fs.mount(
        source = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/',
        mount_point = f'/mnt/{storage_account_name}/{container_name}',
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls_container('formula1adls22', 'raw')

# COMMAND ----------

mount_adls_container('formula1adls22', 'processed')

# COMMAND ----------

mount_adls_container('formula1adls22', 'presentation')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1adls22/test'))

# COMMAND ----------

display(spark.read.csv('/mnt/formula1adls22/test/circuits.csv'))
