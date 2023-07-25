# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'parquet')\
    .option('cloudFiles.schemaLocation', '/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS')\
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")

# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr("Code as code","Description as description", "to_date(Date_Part) as Date_Part", "_rescued_data as rescued_data" )



df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation', '/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS')\
    .start('/mnt/cleansed_datalake/unique_carriers')



# COMMAND ----------

df_base.printSchema()

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/unique_carriers')

# COMMAND ----------

schema=prep_schema(df)
f_delta_cleansed_load("unique_carriers", "/mnt/cleansed_datalake/unique_carriers", schema, 'cleansed_flight')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from cleansed_flight.unique_carriers

# COMMAND ----------


