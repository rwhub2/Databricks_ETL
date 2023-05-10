# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### AutoLoader Pipeline - Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <font color='red'><b> 1. BRONZE LAYER - ingest data from data lake

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG medallion_architecture;
# MAGIC
# MAGIC USE staging

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dimCustomer;
# MAGIC DROP TABLE IF EXISTS silver_dimcustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS staging.dimCustomer (
# MAGIC   CustomerKey STRING,
# MAGIC 	GeographyKey STRING,
# MAGIC 	CustomerAlternateKey STRING, 
# MAGIC 	Title STRING,
# MAGIC 	FirstName STRING,
# MAGIC 	MiddleName STRING,
# MAGIC 	LastName STRING,
# MAGIC 	NameStyle STRING,
# MAGIC 	BirthDate STRING,
# MAGIC 	MaritalStatus STRING,
# MAGIC 	Suffix STRING,
# MAGIC 	Gender STRING,
# MAGIC 	EmailAddress STRING,
# MAGIC 	YearlyIncome STRING,
# MAGIC 	TotalChildren STRING,
# MAGIC 	NumberChildrenAtHome STRING,
# MAGIC 	EnglishEducation STRING,
# MAGIC 	SpanishEducation STRING,
# MAGIC 	FrenchEducation STRING,
# MAGIC 	EnglishOccupation STRING,
# MAGIC 	SpanishOccupation STRING,
# MAGIC 	FrenchOccupation STRING,
# MAGIC 	HouseOwnerFlag STRING,
# MAGIC 	NumberCarsOwned STRING,
# MAGIC 	AddressLine1 STRING,
# MAGIC 	AddressLine2 STRING,
# MAGIC 	Phone STRING,
# MAGIC 	DateFirstPurchase STRING,
# MAGIC 	CommuteDistance STRING
# MAGIC ) using DELTA

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/input/dimCustomer.csv', header=True)

display(df.count())

# COMMAND ----------

df.write.format("delta").table('staging.dimCustomer')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "parquet")\
  .option("cloudFiles.schemaLocation",'dbfs:/mnt/input/schema')\
  .load("dbfs:/mnt/input/*.parquet")\
  .drop(col("_rescued_data"))\
  .writeStream.option("checkpointLocation", 'dbfs:/mnt/input/checkpoint')\
  .table("dimCustomer")

                # .option("cloudFiles.inferColumnTypes", True)\
                # .option("cloudFiles.maxFilesPerTrigger", "1")\

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM dimcustomer;
# MAGIC
# MAGIC -- SELECT * FROM dimCustomer
# MAGIC -- order by CustomerKey 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <font color='red'><b> 5. SILVER LAYER - transform data

# COMMAND ----------

(spark.readStream 
    .table("dimCustomer")  
        .withColumn("CustomerKey", col("CustomerKey").cast(IntegerType()))
        .withColumn("GeographyKey", col("GeographyKey").cast(IntegerType()))       

        .withColumn("NameStyle", col("NameStyle").cast(BooleanType()))
        .withColumn("BirthDate", col("BirthDate").cast(DateType()))

        .withColumn("EmailAddress",  sha1(col("EmailAddress")))
        .withColumn("YearlyIncome", col("YearlyIncome").cast(DecimalType(scale=2)))
        .withColumn("TotalChildren", col("TotalChildren").cast(IntegerType())) 
        .withColumn("NumberChildrenAtHome", col("NumberChildrenAtHome").cast(IntegerType())) 
        .withColumn("HouseOwnerFlag", col("HouseOwnerFlag").cast(IntegerType())) 
        .withColumn("NumberCarsOwned", col("NumberCarsOwned").cast(IntegerType())) 
        .withColumn("DateFirstPurchase", col("DateFirstPurchase").cast(DateType()))
    .writeStream
      .option("checkpointLocation", 'dbfs:/mnt/input/checkpoint_silver')
      .table("silver_dimCustomer")
)




# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM silver_dimcustomer;
# MAGIC
# MAGIC SELECT * FROM silver_dimcustomer
# MAGIC ORDER BY CustomerKey ASC

# COMMAND ----------

cols = silver_customer.columns

recols = cols[0:12] + [cols[-1]] + cols[13:29]

# new

silver_customer = silver_customer.select(recols)

display(silver_customer)

# COMMAND ----------

silver_customer.write.format("delta").saveAsTable("silver_dimCustomer")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT *
# MAGIC -- FROM silver_dimcustomer
# MAGIC -- order by CustomerKey desc
# MAGIC
# MAGIC
# MAGIC -- DROP TABLE silver_dimcustomer

# COMMAND ----------

# qf = spark.read.table("silver_dimcustomer")

display(qf)

# COMMAND ----------

qf.count()

new = qf.na.drop(subset="CustomerKey",how="all")

# COMMAND ----------

new.count()

display(new)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <font color='red'><b> 6. GOLD LAYER - FINAL data

# COMMAND ----------


