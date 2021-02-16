package es.cherrejim.training.spark.training.deltaLake

import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import org.apache.spark.sql.functions.{when, _}

class DeltaTrainingSchemaEnforcementTest extends SharedSparkSessionHelper {

  private val dataBatch1File = "data-batch1.csv"
  private val dataBatch2File = "data-batch2.csv"

  it should "write one csv file " in {

    val path = getClass.getResource(dataBatch1File)

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path.toString)
    df.show()
    import testImplicits._

    val df1 =
      df.select($"FName", $"LName", $"Phone", $"Age", (when($"Age" > 50, "Old").otherwise("Young")).alias("AgeGroup"))
    df1.write.format("parquet").mode("append").save("/tmp/test-3-out")

    df1.show()

    val df2 = spark.read.format("parquet").load("/tmp/test-3-out")
    df2.show
  }

  it should "write one csv file " in {

    val path = getClass.getResource(dataBatch2File)

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path.toString)
    df.show()
    import testImplicits._

    val df1 =
      df.select($"FName", $"LName", $"Phone", $"Age", (when($"Age" > 50, "Old").otherwise("Young")).alias("AgeGroup"))
    df1.write.format("parquet").mode("append").save("/tmp/test-3-out")

    df1.show()

    // No falla porque valida solo al leer al esquema

  }

  it should "write one csv file " in {

    val df2 = spark.read.format("parquet").load("/tmp/test-3-out")
    df2.show //FAils
  }

}
