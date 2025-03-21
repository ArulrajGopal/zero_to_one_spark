// Databricks notebook source
import org.apache.spark.sql.functions.{row_number,col,desc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType}

// COMMAND ----------

val employee_data = Seq(
  (1, "Alice", "2023-10-01", 500),
  (2, "Bob", "2023-10-02", 700),
  (3, "Charlie", "2023-10-03", 800),
  (4, "David", "2023-10-04", 450),
  (5, "Eve", "2023-10-05", 650),
  (6, "Frank", "2023-10-06", 600),
  (7, "Grace", "2023-10-07", 300),
  (8, "Hank", "2023-10-08", 500),
  (9, "Ivy", "2023-10-09", 1000),
  (10, "Jack", "2023-10-10", 900),
  (11, "Karen", "2023-10-11", 700),
  (12, "Leo", "2023-10-12", 550),
  (13, "Mona", "2023-10-13", 450),
  (14, "Nina", "2023-10-14", 800),
  (15, "Omar", "2023-10-15", 750),
  (16, "Paul", "2023-10-16", 850),
  (17, "Quinn", "2023-10-17", 950),
  (18, "Rita", "2023-10-18", 600),
  (19, "Steve", "2023-10-19", 400),
  (20, "Tom", "2023-10-20", 300)
)

val employee_schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = true),
  StructField("Date", StringType, nullable = true),
  StructField("salary", IntegerType, nullable = true),

))

val employee_df = spark.createDataFrame(
  spark.sparkContext.parallelize(employee_data.map(Row.fromTuple)),
  employee_schema
)

val employee_df_with_date = employee_df.withColumn("Date", col("Date").cast(DateType))


// COMMAND ----------

val employee_dept = Seq(
  (1, "Alice", "A"),
  (2, "Bob", "B"),
  (3, "Charlie", "A"),
  (4, "David", "C"),
  (5, "Eve", "B"),
  (6, "Frank", "A"),
  (7, "Grace", "C"),
  (8, "Hank", "B"),
  (9, "Ivy", "A"),
  (10, "Jack", "C"),
  (11, "Karen", "B"),
  (12, "Leo", "A"),
  (13, "Mona", "B"),
  (14, "Nina", "A"),
  (15, "Omar", "C"),
  (16, "Paul", "A"),
  (17, "Quinn", "B"),
  (18, "Rita", "C"),
  (19, "Steve", "A"),
  (20, "Tom", "C")
)

val employee_dept_schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = true),
  StructField("Department", StringType, nullable = true)

))

val employee_dept_df = spark.createDataFrame(
  spark.sparkContext.parallelize(employee_dept.map(Row.fromTuple)),
  employee_dept_schema
)


// COMMAND ----------

val joinedDF = employee_df_with_date.alias("LH")
                    .join(employee_dept_df.alias("RH"), "id").select("LH.id","LH.name","LH.date","LH.salary","RH.department")


val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))
val dfWithRank = joinedDF.withColumn("rank", row_number().over(windowSpec))
val final_df = dfWithRank.filter(col("rank")===1).drop("rank")

// COMMAND ----------

display(final_df)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


