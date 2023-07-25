# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/raw_datalake/airlines/')

# COMMAND ----------

from pyspark.sql.functions import explode
df1=df.select( explode('response'), "Date_Part")
df_final=df1.select("col.*", "Date_Part")

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.write.format('delta').mode('overwrite').save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/airline')
schema=prep_schema(df)


# COMMAND ----------

f_delta_cleansed_load("airline", "/mnt/cleansed_datalake/airline", schema, 'cleansed_flight')

# COMMAND ----------


