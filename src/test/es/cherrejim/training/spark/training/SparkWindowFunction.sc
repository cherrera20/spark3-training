import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("training").master("local").getOrCreate()

import spark.implicits._

val customers = Seq(("Alice", "2016-05-01", 50.00),
  ("Alice", "2016-05-03", 45.00),
  ("Alice", "2016-05-04", 55.00),
  ("Bob", "2016-05-01", 25.00),
  ("Bob", "2016-05-04", 29.00),
  ("Bob", "2016-05-06", 27.00)).toDF("name", "date", "amountSpent")

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Create a window spec.
val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)

// In this window spec, the data is partitioned by customer. Each customer’s data is ordered by date.
// And, the window frame is defined as starting from -1 (one row before the current row)
// and ending at 1 (one row after the current row), for a total of 3 rows in the sliding window.

customers.withColumn( "movingAvg",
  avg(customers("amountSpent")).over(wSpec1)  ).show()
