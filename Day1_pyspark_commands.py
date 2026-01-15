# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# first load data from volume
df_oct = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",header=True,inferSchema=True)


# COMMAND ----------

display(df_oct.limit(5))

# COMMAND ----------

df_oct.describe()

# COMMAND ----------

df_oct.printSchema()

# COMMAND ----------

#find distinct category_code
df_oct.select("category_code").distinct().show()

# COMMAND ----------

# select required columns only
df_oct.select("event_type","user_id","product_id","price").limit(2).show()

# COMMAND ----------

df_oct.filter(df_oct["event_type"] == "view").limit(2).show()

# COMMAND ----------

null_chks = df_oct.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_oct.columns])
null_chks.show()

# COMMAND ----------

# find duplicate rows 
total_rows = df_oct.count()
total_distinct_rows = df_oct.distinct().count()
print("Total rows: ", total_rows)
print("Total distinct rows: ", total_distinct_rows)

# remove duplicate rows
#df_oct_no_dups = df_oct.dropDuplicates()

# COMMAND ----------

dup_rows = (df_oct.groupBy(df_oct.columns).count().filter(F.col("count") > 1).count())
display(dup_rows)

# COMMAND ----------

