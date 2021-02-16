package es.cherrejim.training.spark.training

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.reflect.io.Directory

trait SharedSparkSessionHelper extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  private val _spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("training-spark")
    .enableHiveSupport()
    .config(sparkConf)
    .getOrCreate()

  protected var path: String                 = _
  protected implicit def spark: SparkSession = _spark

  protected def sparkContext: SparkContext = _spark.sparkContext

  protected def sparkConf: SparkConf =
    new SparkConf()
    //To allow use DELETE
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // To allow add column auto
      .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      // To connect hive metastore in postgres
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("hive.metastore.uris", "thrift://localhost:9083")
      /*      .set(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:postgresql://localhost:6432/metastore"
      )
      .set(
        "spark.hadoop.javax.jdo.option.ConnectionDriverName",
        "org.postgresql.Driver"
      )
      .set("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .set("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")*/

      //  .set("spark.sql.parquet.mergeSchema", "true")

      //.set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("hive.stats.jdbc.timeout", "80")
  //.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    path = Files.createTempDirectory(this.getClass.getName).toString
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    new Directory(new File(path)).deleteRecursively()

    spark.sharedState.cacheManager.clearCache()
    spark.sessionState.catalog.reset()
  }

  protected object testImplicits extends SQLImplicits {
    // scalastyle:off method.name
    protected override def _sqlContext: SQLContext = _spark.sqlContext
    // scalastyle:on method.name
  }
}
