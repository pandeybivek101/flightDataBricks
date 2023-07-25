# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'parquet')\
    .option('cloudFiles.schemaLocation', '/dbfs/FileStore/tables/schema/Cancellation')\
    .load("/mnt/raw_datalake/Cancellation/")

# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr("Code as code", "	Description as 	description", "to_date(Date_Part) as Date_Part")



df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation', '/dbfs/FileStore/tables/checkpointLocation/Cancellation')\
    .start('/mnt/cleansed_datalake/cancellation')

# COMMAND ----------

df_base.printSchema()

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/cancellation')

# COMMAND ----------

schema=prep_schema(df)
f_delta_cleansed_load("cancellation", "/mnt/cleansed_datalake/cancellation", schema, 'cleansed_flight')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from cleansed_flight.cancellation

# COMMAND ----------


