// Databricks notebook source
import org.apache.spark.sql.functions.{row_number,col,desc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType}
import org.apache.sql.sql.streaming._

// COMMAND ----------

// val read_df = spark.readStream.format("cloudFiles")
//                       .option("cloudFiles.format","parquet")
//                       .option("cloudFiles.useIncrementalListing","auto")

// val add_timestamp_df = read_df.withColumn("load_date",date_format(current_timestamp.cast("String"),"yyyyMMddHH"))


// add_timestamp_df.writeStream
//                 .trigger(Trigger.AvailableNow())
//                 .option("checkpointLocation", "path")
//                 .foreachBatch(writeFunction _)
//                 .start().awaitTermination()
