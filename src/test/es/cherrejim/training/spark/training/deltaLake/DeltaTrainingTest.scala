package es.cherrejim.training.spark.training.deltaLake

import es.cherrejim.training.spark.training.SharedSparkSessionHelper

class DeltaTrainingTest extends SharedSparkSessionHelper {

  it should "write one csv file " in {
    spark.range(100).repartition(1).write.mode("overwrite").csv("/tmp/tomate/")
  }

  it should "result in data loss" in {
    import testImplicits._

    scala.util.Try(
      spark
        .range(100)
        .repartition(1)
        .map { i =>
          if (i > 50) {
            Thread.sleep(10000)
            throw new RuntimeException("Ops!")
          }
          i
        }
        .write
        .mode("append")
        .csv("tmp/tomate/"))
  }

  it should "write one csv file with delta format" in {
    spark.range(100).repartition(1).write.mode("overwrite").format("delta").save("/tmp/tomate-delta/")
  }

  it should "result in no data loss" in {
    import testImplicits._

    scala.util.Try(
      spark
        .range(100)
        .repartition(1)
        .map { i =>
          if (i > 50) {
            Thread.sleep(5000)
            throw new RuntimeException("Ops!")
          }
          i
        }
        .write
        .mode("overwrite")
        .format("delta")
        .save("/tmp/tomate-delta/")
    )
  }

}
