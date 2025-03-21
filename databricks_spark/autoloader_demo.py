# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    ""
)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("emp_city", StringType(), True)
])

# COMMAND ----------

df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .schema(schema)\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/emp_details/")

# COMMAND ----------

df.writeStream.format("delta")\
    .option("checkpointLocation","wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/raw_emp_details_chk/")\
    .start("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/raw_emp_details/")

# COMMAND ----------

spark.read.format("delta").load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/raw_emp_details/").display()
