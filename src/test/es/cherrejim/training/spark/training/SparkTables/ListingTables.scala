import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import org.scalatest.matchers.should.Matchers

class ListingTables extends SharedSparkSessionHelper with Matchers {

  import testImplicits._
  import scala.util.matching.Regex

  val numberPattern: Regex = "(landing)_([a-z_]*)_(events_domain)".r
  val aPattern: Regex      = "(landing_)([a-z_]*)".r

  it should "bla bla bvla bla" in {

    val databases = Seq("landing_infojobs_it_events_domain",
                        "landing_infojobs_events_domain",
                        "landing_real_estate_events_domain").toDF("databaseName")

    databases.createOrReplaceGlobalTempView("asdasd")
    val groupIdx: Int = 2

    import org.apache.spark.sql.functions.regexp_extract
    import org.apache.spark.sql.functions.split

    val split_col = split(databases.col("databaseName"), "landing")

    import org.apache.spark.sql.functions.udf
    import org.apache.spark.sql.functions.collect_set

    def composeHourly = (cadena: String) => {
      val regex = "landing_"
      val res   = cadena.split(regex)
      s"${regex}hourly_${res.mkString}"
    }

    def getTables = (cadena: String) => {
      val df: String = spark.sql(s"show tables in default").collect().mkString(",")

      "hola"
    }

    val udf  = spark.udf.register("composeHourly", composeHourly)
    val udf2 = spark.udf.register("getTables", getTables)

    val newDF =
      databases
        .withColumn("provider", regexp_extract(databases.col("databaseName"), numberPattern.toString(), 2))
        .withColumn("databaseHourlyName", udf(databases.col("databaseName")))
        .withColumn("tables", udf2(databases.col("databaseName")))

    newDF.show(false)

  }

}
