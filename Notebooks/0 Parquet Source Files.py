# Databricks notebook source
df = spark.read.csv("dbfs:/mnt/input/customer par/dimCustomer_split3.csv", header=True)



# COMMAND ----------

display(df.count())

# COMMAND ----------

df.write.parquet("dbfs:/mnt/input/customer par/quet3")

# COMMAND ----------


