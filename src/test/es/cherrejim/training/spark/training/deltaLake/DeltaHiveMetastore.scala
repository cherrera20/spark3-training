package es.cherrejim.training.spark.training.deltaLake

import java.nio.file.{Files, Paths}

import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers._

class DeltaHiveMetastore extends SharedSparkSessionHelper {

  val Delta         = "delta"
  val PathPermanent = "/tmp/delta-trainig/"
  import testImplicits._
  import io.delta.implicits._

  "saveAsTable when format delta and table does not exists" should "create delta table" in {
    Seq(1L -> "a")
      .toDF("v1", "v2")
      .write
      .partitionBy("v2")
      .mode(SaveMode.Overwrite)
      .option("path", path)
      .format(Delta)
      .saveAsTable("tomate")

    DeltaTable.isDeltaTable(path) shouldBe true
  }

  "create empty table with sql when format is delta and table does not exists" should "create delta table" in {
    val tableName = "events"
    spark.sql(
      s"""
           CREATE TABLE $tableName (
                 |      date DATE,
                 |      eventId STRING,
                 |      eventType STRING,
                 |      data STRING)
                 |    USING DELTA
                 |    LOCATION '$path'
         """.stripMargin
    )

    DeltaTable.isDeltaTable(tableName) shouldBe true // only works with path
    spark.table(tableName).isEmpty shouldBe true
  }

  "Writing in delta table" should "write data correctly" in {
    val tableName = "events"
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING DELTA
         """.stripMargin
    )

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .mode(SaveMode.Overwrite)
      .format(Delta)
      .saveAsTable(tableName)

    spark.table(tableName).count() shouldBe 1

    Seq(("2", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .mode(SaveMode.Append)
      .format(Delta)
      .saveAsTable(tableName)

    spark.table(tableName).count() shouldBe 2
    val tableCatalog = spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
    tableCatalog.provider shouldBe Some(Delta)
    tableCatalog.tableType shouldBe CatalogTableType.MANAGED
  }

  "create partitioned delta table with SQL" should "does not create partitions cols in catalog" in {
    val tableName = "events"
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING DELTA
         |    LOCATION '$path'
         |    PARTITIONED BY (data)
         """.stripMargin
    )

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .partitionBy("data")
      .option("path", path)
      .mode(SaveMode.Overwrite)
      .format(Delta)
      .saveAsTable(tableName)

    val tableCatalog = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
    tableCatalog.partitionColumnNames.length shouldBe 0
    // The following demonstrate that partition was created
    Files.exists(Paths.get(s"$path/data=20201010")) shouldBe true
  }

  "change schema in external delta table" should "change schema in delta table out of the box" in {
    val tableName = "events"
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING DELTA
         |    LOCATION '$path'
         """.stripMargin
    )

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .mode(SaveMode.Append)
      .format(Delta)
      .saveAsTable(tableName)

    spark.table(tableName).count() shouldBe 1

    Seq(("2", "type2", "20201011", "anotherProviderId"))
      .toDF("eventId", "eventType", "data", "provider")
      .write
      .mode(SaveMode.Append)
      .format(Delta)
      .saveAsTable(tableName)

    spark.table(tableName).printSchema()
    spark.table(tableName).columns.length shouldBe 4

    // It does not work because provider already exists
    intercept[Exception] {
      spark.sql(s"""
           |ALTER TABLE $tableName ADD COLUMNS (provider STRING)
           |""".stripMargin)
    }
  }

  "change schema with merge in partitioned external delta table" should "FAILS change schema in table" in {
    val tableName = "events"
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING DELTA
         |    PARTITIONED BY (data)
         |    LOCATION '$path'
         """.stripMargin
    )

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .mode(SaveMode.Append)
      .partitionBy("data")
      .delta(path)

    val tableCatalog = spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
    tableCatalog.partitionColumnNames.length shouldBe 0
    // The following demonstrate that partition was created
    Files.exists(Paths.get(s"$path/data=20201010")) shouldBe true

    val newDF = Seq(("2", "type2", "anotherProviderId", "20201011"))
      .toDF("eventId", "eventType", "provider", "data")

    DeltaTable
      .forName(tableName)
      .alias("old")
      .merge(newDF.alias("new"), "old.eventId = new.eventId")
      .whenNotMatched()
      .insertAll()
      .whenMatched()
      .updateAll()
      .execute()

    spark.sql(
      s"""
           DROP TABLE $tableName
         """.stripMargin
    )

    spark.sql(
      s"""
         | CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      provider STRING,
         |      data STRING)
         |    USING DELTA
         |     PARTITIONED BY (data)
         |    LOCATION '$path'
         """.stripMargin
    )

    DeltaTable.forName(tableName).toDF.printSchema()
    DeltaTable.forName(tableName).toDF.show
    spark.sql(s"SELECT * FROM $tableName").show(false)
    spark.sql(s"SELECT * FROM $tableName").columns.length shouldBe 4
    spark.table(tableName).count() shouldBe 2
  }

  "converted parquet table merge in partitioned external delta table" should "change schema in table" in {

    val tableName       = "events"
    val esternalCatalog = spark.sessionState.catalog.externalCatalog

    // CREATE DELTA TABLE
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING PARQUET
         |    PARTITIONED BY (data)
         |    LOCATION '$path'
         """.stripMargin
    )

    // Cuando creamos la tabla como Delta
    // spark.sessionState.catalog.externalCatalog
    // nos devuelve la tabla sin esquema
    assert(esternalCatalog.getTable("default", tableName).schema.isEmpty == false)

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .partitionBy("data")
      .mode(SaveMode.Overwrite)
      .option("path", path)
      .saveAsTable(tableName)

    DeltaTable.convertToDelta(spark, tableName, "data STRING")
    DeltaTable.isDeltaTable(path) shouldBe true
    spark.table(tableName).columns.length shouldBe 3
    spark.table(tableName).count shouldBe 1

    /**
      * Las columnas se mantienen en Hive Metastore
      */
    assert(esternalCatalog.getTable("default", tableName).schema.isEmpty == false)
    esternalCatalog.getTable("default", tableName).schema.fieldNames.length shouldBe (3)

    Seq(("2", "type2", "provider", "20201011"))
      .toDF("eventId", "eventType", "provider", "data")
      .write
      .partitionBy("data")
      .format("delta")
      .mode(SaveMode.Append)
      .option("path", path)
      .option("mergeSchema", true)
      .saveAsTable(tableName)

    spark.table(tableName).count shouldBe 2

    /**
      * Las columnas NO evolucionan en Hive Metastore
      */
    assert(esternalCatalog.getTable("default", tableName).schema.isEmpty == false)
    esternalCatalog.getTable("default", tableName).schema.fieldNames.length shouldBe (3)

    // spark.catalog.refreshTable(tableName) No sirve para nada

    //spark.sql(s"ALTER TABLE $tableName CHANGE data AFTER provider")

    // NO FUNCIONA
    // ojo el schema no le deberia pasar el campo por el cual particiona
    spark.sessionState.catalog.externalCatalog
      .alterTableDataSchema("default",
                            tableName,
                            StructType(spark.table(tableName).schema.filter(e => !e.name.equals("data"))))

    val schema = DeltaTable.forPath(path).toDF.schema

    val catalogTable = new CatalogTable(
      new TableIdentifier(s"default.$tableName"),
      CatalogTableType.EXTERNAL,
      null,
      schema,
      Some("delta"),
      Seq("data")
    )

    //spark.sessionState.catalog.externalCatalog.createTable(catalogTable, true)

    DeltaTable.forPath(path).toDF.count() shouldBe 2
    DeltaTable.forPath(path).toDF.columns.length shouldBe (4)

    spark.table(tableName).columns.length shouldBe 4
    spark.table(tableName).count shouldBe 2

    spark.sql(s"DELETE FROM $tableName where data = '20201012'")
    spark.sql(s"DESCRIBE FORMATTED $tableName").show(false)
  }

  "NO converted parquet table merge in partitioned external delta table" should "change schema in table" in {

    val tableName = "events"

    // CREATE DELTA TABLE
    spark.sql(
      s"""
           CREATE TABLE $tableName (
         |      eventId STRING,
         |      eventType STRING,
         |      data STRING)
         |    USING DELTA
         |    PARTITIONED BY (data)
         |    LOCATION '$path'
         """.stripMargin
    )

    Seq(("1", "type1", "20201010"))
      .toDF("eventId", "eventType", "data")
      .write
      .partitionBy("data")
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("path", path)
      .saveAsTable(tableName)

    DeltaTable.isDeltaTable(path) shouldBe true
    spark.table(tableName).columns.length shouldBe 3

    /**
      * Las columnas estan en Hive Metastore ???
      */
    Seq(("2", "type2", "provider", "20201011"))
      .toDF("eventId", "eventType", "provider", "data")
      .write
      .partitionBy("data")
      .format("delta")
      .mode(SaveMode.Append)
      .option("path", path)
      .option("mergeSchema", true)
      .saveAsTable(tableName)

    spark.catalog.refreshTable(tableName)
    spark.table(tableName).show(false)
    spark.table(tableName).count shouldBe 3
    spark.table(tableName).columns.length shouldBe 4

    spark.sql(s"DELETE FROM $tableName where data = '20201012'")
    spark.sql(s"DESCRIBE FORMATTED $tableName").show(false)
    spark.table(tableName).count shouldBe 2

  }
}
