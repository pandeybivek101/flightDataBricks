# Databricks notebook source
# MAGIC %run /FlightProject/utilities

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv')\
    .option('cloudFiles.schemaLocation', '/dbfs/FileStore/tables/schema/flight')\
    .load("/mnt/raw_datalake/flight/")

# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr("cast('Year' as int) as year", "cast('Month' as int) as month", "cast('DayofMonth' as int) as dayofmonth",
                      "cast('DayOfWeek' as int) as dayofweek", "cast('DepTime' as int) as deptime", "cast('CRSDepTime' as int) as crsSdeptime",
                       "cast('ArrTime' as int) as arrtime", "cast('CRSArrTime' as int) as crsarrtime", "cast('UniqueCarrier' as int) as uniquecarrier",
                      "cast('FlightNum' as int) as flightnum", "cast('TailNum' as int) as tailnum", "cast('ActualElapsedTime' as int) as actualelapsedtime",
                      "cast('CRSElapsedTime' as int) as crselapsedtime", "cast('AirTime' as int) as airtime", "cast('ArrDelay' as int) as arrdelay",
                      "cast('DepDelay' as int) as depdelay", "Origin as origin", "Dest as dest",
                      "cast('Distance' as int) as distance", "cast('TaxiIn' as int) as taxiin", "cast('TaxiOut' as int) as taxiOut",
                      "cast('Cancelled' as int) as cancelled", "CancellationCode as cancellationcode", "cast('Diverted' as int) as diverted",
                      "cast('CarrierDelay' as int) as carrierdelay", "cast('WeatherDelay' as int) as weatherdelay", "cast('NASDelay' as int) as nasdelay",
                      "cast('SecurityDelay' as int) as securitydelay", "cast('LateAircraftDelay' as int) as lateaircraftdelay",
                      "_rescued_data as rescued_data", "to_date('Date-part') as Date_Part")



df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation', '/dbfs/FileStore/tables/checkpointLocation/flight')\
    .start('/mnt/cleansed_datalake/flight')



# COMMAND ----------

df_base.printSchema()

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleansed_datalake/flight')

# COMMAND ----------

schema=prep_schema(df)
f_delta_cleansed_load("flight", "/mnt/cleansed_datalake/flight", schema, 'cleansed_flight')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from cleansed_flight.flight

# COMMAND ----------


