// Databricks notebook source
import org.apache.spark.sql.functions.{current_timestamp, col, explode, split,to_json, struct}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}

// COMMAND ----------

val emp_schema = StructType(Array(
  StructField("empid", StringType, true),
  StructField("name", StringType, true),
  StructField("designation", StringType, true),
  StructField("salary", IntegerType, true),
  StructField("city", StringType, true),
  StructField("department", StringType, true)
))

// COMMAND ----------

val df = spark.readStream
              .format("csv")
              .schema(emp_schema)
              .option("Header",true)
              .load("/mnt/test/raw/employee_details")


val dfWithLoadedTime = df.withColumn("loaded_time", current_timestamp())

val filteredDf = dfWithLoadedTime.filter(!col("department").equalTo("Finance"))

// COMMAND ----------

val query = filteredDf
  .writeStream
  .format("delta")
  .option("path", "/mnt/test/curated/employee_details")
  .option("checkpointLocation", "/mnt/test/checkpoints/employee_details_curated")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

// COMMAND ----------

val endpoint = "sb://arul-event-hub.servicebus.windows.net/"
val sharedAccessKeyName = "event-hub-policy"
val sharedAccessKey = ""
val entityPath = "event-hub-1"

val connectionString = ConnectionStringBuilder(s"Endpoint=$endpoint;SharedAccessKeyName=$sharedAccessKeyName;SharedAccessKey=$sharedAccessKey;EntityPath=$entityPath")
  .setEventHubName(entityPath)
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

val dfWithBody = filteredDf.withColumn("body", to_json(struct(df.columns.map(col): _*)))

val query = dfWithBody
  .writeStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .option("checkpointLocation", "/mnt/test/checkpoints/employee_details")
  .start()

  

// COMMAND ----------

val df = spark.read.format("delta").load("/mnt/test/curated/employee_details")
display(df)
