# Databricks notebook source
# MAGIC %scala
# MAGIC val storageAccount = dbutils.secrets.get("flight-project-scope", "blob-source-storage")
# MAGIC val container = dbutils.secrets.get("flight-project-scope", "blob-container")
# MAGIC val sasKey = dbutils.secrets.get("flight-project-scope", "blob-source-sas-token")
# MAGIC val sourceString = "wasbs://"+container+"@"+storageAccount+".blob.core.windows.net/"
# MAGIC
# MAGIC val mountPoint = "/mnt/source_blob"
# MAGIC val confKey = "fs.azure.sas."+container+"."+storageAccount+".blob.core.windows.net"
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC     source = sourceString,
# MAGIC     mountPoint = mountPoint,
# MAGIC     extraConfigs = Map(confKey -> sasKey)
# MAGIC   )

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls "/mnt/source_blob"

# COMMAND ----------

print("dd")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get("flight-project-scope", "data-app-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="flight-project-scope",key="data-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="flight-project-scope",key="data-lakesink-endpoint")}

mountPoint = "/mnt/raw_datalake"

# Optionally, you can add <directory-name> to the source URI of your mount point.
# if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mount()):
# if mountPoint not in dbutils.fs.mount():
dbutils.fs.mount(
    source = dbutils.secrets.get(scope="flight-project-scope",key="data-lakesink-url"),
    mount_point = mountPoint,
    extra_configs = configs
    )


 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/raw_datalake"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get("flight-project-scope", "data-app-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="flight-project-scope",key="data-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="flight-project-scope",key="data-lakesink-endpoint")}

mountPoint = "/mnt/cleansed_datalake"

# Optionally, you can add <directory-name> to the source URI of your mount point.
# if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mount()):
# if mountPoint not in dbutils.fs.mount():
dbutils.fs.mount(
    source = dbutils.secrets.get(scope="flight-project-scope",key="data-cleansed-url"),
    mount_point = mountPoint,
    extra_configs = configs
    )
    


# COMMAND ----------


