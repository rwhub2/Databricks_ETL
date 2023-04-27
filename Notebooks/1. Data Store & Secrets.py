# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Create Data Store & Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <font color='red'><b> 1. Secret Scope

# COMMAND ----------

# dbutils.secrets.list('databricksSecrets')

dbn = dbutils.secrets.get('databricksSecrets','sqldatabase')
usern = dbutils.secrets.get('databricksSecrets','userName')
passn = dbutils.secrets.get('databricksSecrets','password')

# display(usern)

# COMMAND ----------

dbutils.secrets.list('databricksSecrets')

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

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={dbn};user={usern};password={passn}"


# #property values
# jdbcDatabase = 'Backup_AdventureWorksDW2019'
# jdbcUsername = 'sauser'
# jdbcPassword = 'Adventure9#'



# The JDBC URL of the form jdbc:subprotocol:subname to connect to. The source-specific connection properties may be specified in the URL. e.g., jdbc:postgresql://localhost/test?user=fred&password=secret

# jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]


#########################################################################################################
# METHOD 2

connectionString = f'jdbc:sqlserver://dbsserver.database.windows.net:1433;database={dbn};user={usern};password={passn};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'

# jdbc:sqlserver://dbsserver.database.windows.net:1433;database=Backup_AdventureWorksDW2019;user=sauser@dbsserver;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;

# COMMAND ----------

display()

# COMMAND ----------

# METHOD 1

df = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable","dbo.DimProduct").load()

display(df.limit(5))

# COMMAND ----------

# METHOD 2

df1 = spark.read.jdbc(connectionString, "dbo.DimProduct")

display(df1.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <font color='red'><b> 3. Mount Object Store

# COMMAND ----------

# DATA LAKE MOUNT

# dbutils.fs.help("mount")

dbutils.fs.mount(
    'adl://input@sinkadls.dfs.core.windows.net/'
    ,"/mnt/sinkblob"
    ,extra_configs = {"fs.azure.account.key.sinkadls.dfs.core.windows.net/":"wz808VyRGO88Lw3Y/eB4JOJx4eVJodU3Y9g3rTwWH3abQ/Yqb/mpC24WCZD9V7ozSu6nSCrXsGm8+ASt6SXPuQ=="}
)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/mnt", True)

dbutils.fs.unmount("/mnt/")

# COMMAND ----------

# BLOB MOUNT

# dbutils.fs.help("mount")

dbutils.fs.mount(
    source = "wasbs://input@sinkblobb.blob.core.windows.net/input"
    ,mount_point = "/mount/sinkblob"
    ,extra_configs = {"fs.azure.account.key.sinkblobb.blob.core.windows.net/":"t0x/ck6Q7omG76EuFZGxquGvlClrrYWdD0jr/3K0pwFlbIQSRTSXUF6S56TFqzhA19nbILs6j37L+AStToGrqw=="}
)

# COMMAND ----------

# dbutils.fs.help("unmount")

dbutils.fs.unmount("/mnt/sinkblob")

# COMMAND ----------


