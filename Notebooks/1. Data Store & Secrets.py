# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Create Data Store & Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <font color='red'><b> 1. Secret Scope

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <font color='red'><b> 2. Connect to JDBC

# COMMAND ----------

#########################################################################################################
# METHOD 1

#serverName & portNumber
jdbcHostname = "dbsserver.database.windows.net"
jdbcPort = 1433

#property values
jdbcDatabase = 'Backup_AdventureWorksDW2019'
jdbcUsername = 'sauser'
jdbcPassword = 'Adventure9#'

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"


# The JDBC URL of the form jdbc:subprotocol:subname to connect to. The source-specific connection properties may be specified in the URL. e.g., jdbc:postgresql://localhost/test?user=fred&password=secret

# jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]


#########################################################################################################
# METHOD 2

connectionString = 'jdbc:sqlserver://dbsserver.database.windows.net:1433;database=Backup_AdventureWorksDW2019;user=sauser@dbsserver;password=Adventure9#;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'



# COMMAND ----------

# METHOD 1

df = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable","dbo.DimProduct").load()

display(df)

# COMMAND ----------

# METHOD 2

df1 = spark.read.jdbc(connectionString, "dbo.DimProduct")

display(df1)

# COMMAND ----------


