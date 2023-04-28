# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Medallion Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <span style="color: red"><b> 1. Secret Scope</span>

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

dbutils.secrets.list("databricksSecrets")

# COMMAND ----------

# help(spark.conf)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth"
,"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
,"fs.azure.account.oauth2.client.id": "634d8a52-7fcb-4126-ae0b-441246f3508d"
,"fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="databricksSecrets",key="service-cred")
,"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/eaf52279-66e9-4cb9-9041-3c8e29d0cdd1/oauth2/token"
,"fs.azure.createRemoteFileSystemDuringInitialization":"true"
}

# COMMAND ----------

# DATA LAKE MOUNT

# dbutils.fs.help("mount")

dbutils.fs.mount(
    source = 'abfss://input@sinkadls.dfs.core.windows.net/'
    ,mount_point = "/mnt/sinkblob"
    ,extra_configs = configs
)



# COMMAND ----------


