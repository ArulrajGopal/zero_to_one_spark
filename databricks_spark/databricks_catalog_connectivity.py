# Databricks notebook source
# MAGIC %md
# MAGIC #reading from adls

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    ""
)

user_df = spark.read\
        .format("csv")\
        .option("Header",False)\
        .option("inferSchema",True)\
        .option("sep", "|")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/movie_lens/ml-100k/user.user")

# COMMAND ----------

# MAGIC %md
# MAGIC #column renaming

# COMMAND ----------

col_map = {
    "_c0": "userId",
    "_c1": "age",
    "_c2": "gender",
    "_c3": "occupation",
    "_c4": "zip_code"
}

# COMMAND ----------

def col_renaming(df, col_map):
  list = []
  for i, j in col_map.items():
    list.append(f"{i} as {j}")

  renamed_df = df.selectExpr(*list)

  return renamed_df

# COMMAND ----------

final_df = col_renaming(user_df, col_map)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE HIVE_METASTORE;
# MAGIC CREATE DATABASE IF NOT EXISTS DEMO;
# MAGIC USE DEMO;

# COMMAND ----------

# MAGIC %md
# MAGIC #writing into catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.demo.users_details;
# MAGIC
# MAGIC CREATE TABLE hive_metastore.demo.users_details
# MAGIC (
# MAGIC   userId INT,
# MAGIC   age INT,
# MAGIC   gender STRING,
# MAGIC   occupation STRING,
# MAGIC   zip_code STRING
# MAGIC )
# MAGIC using delta

# COMMAND ----------

final_df.write.mode("overwrite").saveAsTable("hive_metastore.demo.users_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.demo.users_details

# COMMAND ----------


