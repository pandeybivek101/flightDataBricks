# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\
    .option('cloudFiles.schemaLocation', '/dbfs/FileStore/tables/schema/PLANE')\
    .load("/mnt/raw_datalake/PLANE/")

# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr("tailnum as tailid", "type", "manufacturer", "to_date(issue_date) as issue_date", "model", "status", "aircraft_type","cast('year' as int) as year", "to_date(Date_Part) as Date_Part")



df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation', '/dbfs/FileStore/tables/checkpointLocation/PLANE')\
    .start('/mnt/cleansed_datalake/plane')

# COMMAND ----------

df_base.printSchema()

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/plane')

# COMMAND ----------

schema=prep_schema(df)
f_delta_cleansed_load("plane", "/mnt/cleansed_datalake/plane", schema, 'cleansed_flight')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from cleansed_flight.plane

# COMMAND ----------


