# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    ""
)


# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS demo_catalog")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_catalog.demo_database")

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, split, rank
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

# COMMAND ----------

stage_table = "stage_employee"
curated_table = "curated_employee"


schema = StructType([
    StructField("EmpID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True)
])

@dlt.table(
  name= stage_table,
  comment="raw table read from employee details file"
)

def stage():
    raw_df = spark.read\
            .format("csv")\
            .schema(schema)\
            .option("Header",True)\
            .option("sep", "~")\
            .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/emp_details/emp_with_salary.txt")

    stage_df = raw_df.withColumn("FirstName", split(col("Name"), " ").getItem(0))\
                    .withColumn("LastName", split(col("Name"), " ").getItem(1))\
                    .drop("Name")

    
    return stage_df


@dlt.table(
  name=curated_table,
  comment="aggregated table"
)

@dlt.expect("valid_salary", "Salary > 0")

def curated():
  windowSpec = Window.partitionBy("Department").orderBy(col("Salary").desc())
  ranked_df = dlt.read(stage_table).withColumn("Rank", rank().over(windowSpec))
  final_df = ranked_df.filter(col("Rank")<2).drop("Rank")

  return final_df


# COMMAND ----------

'''
import dlt
from pyspark.sql.functions import *

import os

os.environ["UNITY_CATALOG_VOLUME_PATH"] = "/Volumes/<catalog-name>/<schema-name>/<volume-name>/"
os.environ["DATASET_DOWNLOAD_URL"] = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
os.environ["DATASET_DOWNLOAD_FILENAME"] = "rows.csv"

dbutils.fs.cp(f"{os.environ.get('DATASET_DOWNLOAD_URL')}", f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}")


@dlt.table(
  comment="Popular baby first names in New York. This data was ingested from the New York State Department of Health."
)
def baby_names_raw():
  df = spark.read.csv(f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}", header=True, inferSchema=True)
  df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
  return df_renamed_column


@dlt.table(
  comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "First_Name IS NOT NULL")
@dlt.expect_or_fail("valid_count", "Count > 0")
def baby_names_prepared():
  return (
    dlt.read("baby_names_raw")
      .withColumnRenamed("Year", "Year_Of_Birth")
      .select("Year_Of_Birth", "First_Name", "Count")
  )

@dlt.table(
  comment="A table summarizing counts of the top baby names for New York for 2021."
)
def top_baby_names_2021():
  return (
    dlt.read("baby_names_prepared")
      .filter(expr("Year_Of_Birth == 2021"))
      .groupBy("First_Name")
      .agg(sum("Count").alias("Total_Count"))
      .sort(desc("Total_Count"))
      .limit(10)
  )

'''

