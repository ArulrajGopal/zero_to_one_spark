# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import to_timestamp, col

# COMMAND ----------

import requests

url = "https://latest-stock-price.p.rapidapi.com/equities"

headers = {
	"x-rapidapi-key": "23a945bde0msha8c00c2faa561f5p119d77jsnea9895e0d88d",
	"x-rapidapi-host": "latest-stock-price.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
output = response.json()

# COMMAND ----------

# Define the schema based on the known structure of the data
schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Date/Time", StringType(), True),
    StructField("Total Volume", StringType(), True),
    StructField("Net Change", StringType(), True),
    StructField("LTP", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("High", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Open", StringType(), True),
    StructField("P Close", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("52Wk High", StringType(), True),
    StructField("52Wk Low", StringType(), True),
    StructField("5Year High", StringType(), True),
    StructField("ISIN", StringType(), True),
    StructField("NSE Symbol", StringType(), True),
    StructField("1M High", StringType(), True),
    StructField("3M High", StringType(), True),
    StructField("6M High", StringType(), True),
    StructField("%Chng", StringType(), True)
])

# Convert the list of dictionaries to a DataFrame using the defined schema
raw_df = spark.createDataFrame(output, schema=schema)


# COMMAND ----------

colum_mapping = {
    "Symbol" : "Symbol",
    "Name":"Name",
    "ISIN":"ISIN",
    "NSE Symbol":"NSE Symbol",
    "Date/Time":"DateTime",
    "Total Volume":"TotalVolume",
    "Net Change":"NetChange",
    "LTP":"LastTradedPrice",
    "Volume":"Volume",
    "High":"High",
    "Low":"Low",
    "Open":"Open",
    "P Close":"PreviousClose",
    "52Wk High":"YearHigh",
    "52Wk Low":"YearLow",
    "%Chng":"PercentChange"
}

# COMMAND ----------

rename_lst = []
for old_name, new_name in colum_mapping.items():
    rename_lst.append(f'`{old_name}` as `{new_name}`')

column_renamed_df = raw_df.selectExpr(*rename_lst)

# COMMAND ----------

#data type conversion
date_converted_df =  column_renamed_df.withColumn('DateTime', to_timestamp('DateTime', 'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX'))

# COMMAND ----------

data_type_map = {
  'TotalVolume': 'long',
  'NetChange': 'double',
  'LTP': 'double',
  'Volume': 'long',
  'High': 'double',
  'Low': 'double',
  'Open':'double',
  'PreviousClose':'double',
  'YearHigh':'double',
  'YearLow':'double',
  'PercentChange':'double'
  }

cast_col_list = []

for column_schema in date_converted_df.dtypes:
    cast_col_list.append(col(column_schema[0]).cast(data_type_map.get(column_schema[0], column_schema[1])))

final_df = date_converted_df.select(*cast_col_list)

# COMMAND ----------

options = {
    "sfUrl": "lzhpmlh-jn59373.snowflakecomputing.com",
    "sfUser":"ARULRAJGOPAL",
    "sfPassword":"Arulraj@12345678",
    "sfDatabase":"DEMO_DB",
    "sfSchema":"DEMO_SCHEMA",
    "sfWarehouse":"COMPUTE_WH"
}

# COMMAND ----------

final_df.write.format("snowflake").options(**options).mode("overwrite").option("dbtable", "raw").save()
