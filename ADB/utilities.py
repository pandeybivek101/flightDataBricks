# Databricks notebook source
def prep_schema(df):
    schemalist = df.dtypes
    schema=""
    for index,item in enumerate(schemalist):
        if index != len(schemalist)-1:
            schema+=item[0]+" "+item[1]+", "
        else:
            schema+=item[0]+" "+item[1]
    return schema

# COMMAND ----------

def f_delta_cleansed_load(tablename, location, schema, databasename):
    try:
        spark.sql(f"""
                create table if not exists {databasename}.{tablename}
                ({schema})
                using delta
                location "{location}"
                """
                )
    except Exception as err:
        print("exception occoured "+str(err))

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC spark.sql("""desc history cleansed_flight.airline""").createOrReplaceTempView("Table_count")

# COMMAND ----------

# MAGIC %py
# MAGIC count_current=spark.sql("""select operationMetrics.numOutputRows from Table_count where version=( select max(version) from Table_count where Operation='WRITE')""")
# MAGIC if count_current.first() is None:
# MAGIC     final_count_current = 0
# MAGIC else:
# MAGIC     final_count_current = count_current.first().numOutputRows
# MAGIC
# MAGIC
# MAGIC count_previous=spark.sql("""select operationMetrics.numOutputRows from Table_count where version < ( select version from Table_count where Operation='WRITE' order by version desc limit 1)""")
# MAGIC
# MAGIC if count_previous.first() is None:
# MAGIC     final_count_previous = 0
# MAGIC else:
# MAGIC     final_count_previous = count_previous.first().numOutputRows
# MAGIC
# MAGIC print(f"{final_count_current} {final_count_previous}")
# MAGIC
# MAGIC if(int(final_count_current) -  int(final_count_previous)) > 100:
# MAGIC     print("difference is huge")
# MAGIC else:
# MAGIC     pass

# COMMAND ----------


