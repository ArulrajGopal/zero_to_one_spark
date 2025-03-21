# Databricks notebook source
# MAGIC %md
# MAGIC #Azure SQL connect using username & password

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading

# COMMAND ----------

jdbcHostname = "arulsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database1"
jdbcUsername = "<username>"
jdbcPassword = "<password>"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};"

employee_df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Employees") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", jdbcDriver) \
    .load()

# COMMAND ----------

employee_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# Define the schema for the students dataframe
schema = StructType([
    StructField("student_id", IntegerType(), nullable=False),
    StructField("student_name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("grade", StringType(), nullable=False)
])

# Create the students dataframe with the specified schema and data
students_data = [
    (1, "John Doe", 18, "A"),
    (2, "Jane Smith", 17, "B"),
    (3, "Mike Johnson", 19, "A"),
    (4, "Emily Brown", 16, "C")
]

students_df = spark.createDataFrame(students_data, schema)\
                    .withColumn("current_timestamp", current_timestamp())

students_df.display()

# COMMAND ----------

jdbcHostname = "arulsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "database1"
jdbcUsername = "<username>"
jdbcPassword = "<password>"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};"

students_df.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "Students") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .option("driver", jdbcDriver) \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Azure SQL connect using spn

# COMMAND ----------

# MAGIC %md
# MAGIC ##Note:- 
# MAGIC In case of below failure, then while using service prinicpal to access Azure sql
# MAGIC
# MAGIC -> Go to the azure sql server ->  settings -> Microsoft Entra ID -> Set Admin -> <select the service principal>
# MAGIC
# MAGIC
# MAGIC com.microsoft.sqlserver.jdbc.SQLServerException: Failed to authenticate the user be0a6a77-ef54-4b1c-8641-e3f4d3b5d1c0 in Active Directory (Authentication=ActiveDirectoryServicePrincipal). AADSTS900021: Requested tenant identifier 00000000-0000-0000-0000-000000000000 is not valid. Tenant identifiers may not be an empty GUID. Trace ID: d5119b35-aa35-463f-a019-c0a6a

# COMMAND ----------

# MAGIC %md
# MAGIC #reading

# COMMAND ----------

employee_df = spark.read\
    .format("sqlserver")\
    .option("host", "arulsqlserver.database.windows.net")\
    .option("authentication","ActiveDirectoryServicePrincipal")\
    .option("user", "<app_id>")\
    .option("password", "<secret>")\
    .option("database","database1")\
    .option("dbtable","Employees")\
    .load()


employee_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## writing

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

# Define the schema for the students dataframe
schema = StructType([
    StructField("student_id", IntegerType(), nullable=False),
    StructField("student_name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("grade", StringType(), nullable=False)
])

# Create the students dataframe with the specified schema and data
students_data = [
    (1, "John Doe", 18, "A"),
    (2, "Jane Smith", 17, "B"),
    (3, "Mike Johnson", 19, "A"),
    (4, "Emily Brown", 16, "C")
]

students_df = spark.createDataFrame(students_data, schema)\
                    .withColumn("current_timestamp", current_timestamp())

students_df.display()


# COMMAND ----------

students_df.write\
    .format("sqlserver")\
    .mode("overwrite")\
    .option("host", "arulsqlserver.database.windows.net")\
    .option("authentication","ActiveDirectoryServicePrincipal")\
    .option("user", "<app_id>")\
    .option("password", "<secret>")\
    .option("database","database1")\
    .option("dbtable","students")\
    .save()
