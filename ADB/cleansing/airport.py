# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\
    .option('cloudFiles.schemaLocation', '/dbfs/FileStore/tables/schema/Airport')\
    .load("/mnt/raw_datalake/Airport/")

# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr(
    "Code",
    "split(Description, ',')[0] as city",
    "split(split(Description, ',')[1], ':')[0] as country",
    "split(split(Description, ',')[1], ':')[1] as airport",
    "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part"

    )


df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation', '/dbfs/FileStore/tables/checkpointLocation/Airport')\
    .start('/mnt/cleansed_datalake/airport')

# COMMAND ----------

df_base.printSchema()

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airport')


# COMMAND ----------

schema=prep_schema(df)
f_delta_cleansed_load("airport", "/mnt/cleansed_datalake/airport", schema, 'cleansed_flight')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from cleansed_flight.airport

# COMMAND ----------


