# Databricks notebook source
!pip install tabula


# COMMAND ----------

import tabula
from datetime import date


outputdirs = dbutils.fs.mkdirs(f"/mnt/raw_datalake/PLANE/Date_Part={str(date.today())}/")




# COMMAND ----------

tabula.convert_into("/dbfs/mnt/source_blob/PLANE.pdf", f"/dbfs/mnt/raw_datalake/PLANE/Date_Part={str(date.today())}/PLANE.csv", output_format="csv", pages="all")

# COMMAND ----------



# COMMAND ----------


