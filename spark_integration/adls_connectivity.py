# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS connect through account key without mounting

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "<account-key>"
)

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/emp_details/emp_details.csv")

read_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # ADLS connect through account key with mounting

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

storage_account_name = "arulrajstorageaccount"
accountkey= "account_key"  
container_name = "private"
fullname = "fs.azure.account.key." +storage_account_name+ ".blob.core.windows.net"


dbutils.fs.mount(  
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/sample_employee/emp_details/", 
  mount_point =f"/mnt/{container_name}", 
  extra_configs = {fullname : accountkey}) 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .option("sep", "~")\
        .load("/mnt/private/emp_details.csv")


read_df.display()

# COMMAND ----------

dbutils.fs.unmount('/mnt/private')

# COMMAND ----------

# MAGIC %md
# MAGIC #ADLS connect through service principle

# COMMAND ----------

application_id = "b2bff165-f357-48bb-9553-ffae73e88296"
tenant_id = "0f46c550-7a7d-4630-b67d-3cb63948aa0a"
client_secret = "client_id"
storage_account = "arulrajstorageaccount"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", f"{application_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", f"{client_secret}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("emp_city", StringType(), True)
])

# COMMAND ----------

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .schema(schema)\
        .option("sep", "~")\
        .load("abfss://private@arulrajstorageaccount.dfs.core.windows.net/sample_employee/emp_details/emp_details.csv")

read_df.display()

# COMMAND ----------


