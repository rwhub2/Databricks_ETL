# Databricks notebook source
dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/customer_table.csv", header=True)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE STG

# COMMAND ----------

df.write.format("delta").saveAsTable("stg.Dim_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from stg.dim_customer;
# MAGIC
# MAGIC select * from stg.dim_customer

# COMMAND ----------

poke = spark.read.format("csv").option("header", True).load("dbfs:/FileStore/tables/pokemon_data.csv")

# COMMAND ----------

display(poke)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode","name")

# COMMAND ----------

poke.write.format("delta").save("dbfs:/FileStore/delta/pokeman_list")

# COMMAND ----------

# dbutils.fs.rm("FileStore/delta/pokeman_list", recurse=True)

# COMMAND ----------

from delta import *

# COMMAND ----------

DeltaTable.create(spark).tableName("pokemanss").location("dbfs:/FileStore/delta/pokeman_list").execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from pokemanss
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO pokemanss VALUES ("this is a new insert please")

# COMMAND ----------


