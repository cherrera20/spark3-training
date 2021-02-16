package es.cherrejim.training.spark.training.insertinto_saveastable

import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import es.cherrejim.training.spark.training.utils.DataFrameSchemaUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class UnderstadingInsertInto extends SharedSparkSessionHelper with Matchers {

  val TargetTable = "companies_table"

  "saveAsTable with dynamic partition conf" should "no delete other partitions not present in batch" in {
    import testImplicits._

    val companiesDF =
      Seq(("A", "Company1", 100), ("B", "Company2", 50), ("B", "Company3", 55)).toDF("id", "company", "size")
    companiesDF.write
      .option("path", s"/tmp/chris-test/$TargetTable/")
      .option("format", "text")
      .mode(SaveMode.Overwrite)
      .partitionBy("id", "company")
      .saveAsTable(TargetTable)

    val secondCompaniesDF = Seq(("B", "Company2", 55)).toDF("id", "company", "size")
    secondCompaniesDF.write
      .option("path", s"/tmp/chris-test/$TargetTable/")
      .mode(SaveMode.Overwrite)
      .partitionBy("id", "company")
      .saveAsTable(TargetTable)

    spark.sql(s"SHOW PARTITIONS $TargetTable").show(false)
    spark.sql(s"SHOW PARTITIONS $TargetTable").count() shouldBe 3
  }

  it should "Store table and insert into new record on new partitions" in {
    import testImplicits._

    val companiesDF = Seq(("A", "Company1"), ("B", "Company2")).toDF("id", "company")
    companiesDF.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(TargetTable)
    val companiesHiveDF = spark.sql(s"SELECT * FROM $TargetTable")
    companiesHiveDF.show(false)

    /**
    partition column should be at the end to match table schema:

    val secondCompaniesDF = Seq(("C", "Company3"), ("D", "Company4"))
      .toDF("id", "company")
    secondCompaniesDF.write.mode(SaveMode.Append).insertInto(targetTable)
    val companiesHiveAfterInsertDF = spark.sql(s"SELECT * FROM ${targetTable}")
    companiesHiveAfterInsertDF.show(false)

    /** OJO */
    +--------+--------+
    | company | id |
    +--------+--------+
    | Company2 | B |
    | Company1 | A |
    | D | Company4 |
    | C | Company3 |
    +--------+--------+
    **/
    //partition column should be at the end to match table schema.
    val secondCompaniesDF = Seq(("Company3", "C"), ("Company4", "D"))
      .toDF("company", "id")

    secondCompaniesDF.write.mode(SaveMode.Append).insertInto(TargetTable)
    val companiesHiveAfterInsertDF = spark.sql(s"SELECT * FROM $TargetTable")
    companiesHiveAfterInsertDF.printSchema()
    companiesHiveAfterInsertDF.show(false)

    companiesDF.count() should equal(2)
    companiesHiveAfterInsertDF.count() should equal(4)
    companiesHiveDF.select("id").collect().map(_.get(0)) should contain allOf ("A", "B")
    companiesHiveAfterInsertDF.select("id").collect().map(_.get(0)) should contain allOf ("A", "B", "C", "D")

  }

  "insert into" should "fails when New Batch With Extra Columns" in {
    import testImplicits._

    val companiesDF = Seq(("A", "Company1"), ("B", "Company2")).toDF("id", "company")
    companiesDF.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(TargetTable)
    val companiesHiveDF = spark.sql(s"SELECT * FROM ${TargetTable}")
    companiesHiveDF.show(false)

    //again adding the partition column at the end and trying to overwrite partition C.
    val thirdCompaniesDF = Seq(("Company4", 10, "C"), ("Company5", 20, "F"))
      .toDF("company", "size", "id")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      thirdCompaniesDF.write.mode(SaveMode.Overwrite).insertInto(TargetTable)
    }

  }

  it should "do alter table to allow extra columns " in {
    import testImplicits._

    val companiesDF = Seq(("A", "Company1"), ("B", "Company2")).toDF("id", "company")
    companiesDF.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(TargetTable)

    //partition column should be at the end to match table schema.
    val secondCompaniesDF = Seq(("Company3", "C"), ("Company4", "D"))
      .toDF("company", "id")

    secondCompaniesDF.write.mode(SaveMode.Append).insertInto(TargetTable)

    val tableFlatDF = spark.sql(s"SELECT * FROM $TargetTable limit 1")

    //again adding the partition column at the end and trying to overwrite partition C.
    val thirdCompaniesDF = Seq(("Company4", 10, "C"), ("Company5", 20, "F"))
      .toDF("company", "size", "id")

    val columnsMissingInTable =
      DataFrameSchemaUtils.getMissingTableColumnsAgainstDataFrameSchema(thirdCompaniesDF, tableFlatDF)

    if (columnsMissingInTable.size > 0) {
      spark.sql(
        (s"ALTER TABLE $TargetTable " +
          s"ADD COLUMNS (${columnsMissingInTable.mkString(" , ")})"))
    }

    thirdCompaniesDF.write.mode(SaveMode.Overwrite).insertInto(TargetTable)
    val companiesHiveAfterInsertNewSchemaDF = spark.sql(s"SELECT * FROM $TargetTable")
    companiesHiveAfterInsertNewSchemaDF.printSchema()
    companiesHiveAfterInsertNewSchemaDF.show(false)

  }

  "insert into" should "fails when New Batch With fewer Columns" in {
    import testImplicits._

    val companiesDF = Seq(("A", "Company1"), ("B", "Company2")).toDF("id", "company")
    companiesDF.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(TargetTable)
    val companiesHiveDF = spark.sql(s"SELECT * FROM ${TargetTable}")
    companiesHiveDF.show(false)

    val fourthCompaniesDF = Seq("G", "H")
      .toDF("id")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      fourthCompaniesDF.write.mode(SaveMode.Overwrite).insertInto(TargetTable)
    }

  }

  it should "do alter table to allow fewer columns " in {
    import testImplicits._

    val companiesDF = Seq(("A", "Company1"), ("B", "Company2")).toDF("id", "company")
    companiesDF.write.mode(SaveMode.Overwrite).partitionBy("id").saveAsTable(TargetTable)

    //partition column should be at the end to match table schema.
    val secondCompaniesDF = Seq(("Company3", "C"), ("Company4", "D"))
      .toDF("company", "id")

    secondCompaniesDF.write.mode(SaveMode.Append).insertInto(TargetTable)

    val tableFlatDF = spark.sql(s"SELECT * FROM $TargetTable limit 1")

    //again adding the partition column at the end and trying to overwrite partition C.
    val thirdCompaniesDF = Seq(("Company4", 10, "C"), ("Company5", 20, "F"))
      .toDF("company", "size", "id")

    val columnsMissingInTable =
      DataFrameSchemaUtils.getMissingTableColumnsAgainstDataFrameSchema(thirdCompaniesDF, tableFlatDF)

    if (columnsMissingInTable.size > 0) {
      spark.sql(
        (s"ALTER TABLE $TargetTable " +
          s"ADD COLUMNS (${columnsMissingInTable.mkString(" , ")})"))
    }

    thirdCompaniesDF.write.mode(SaveMode.Overwrite).insertInto(TargetTable)
    val companiesHiveAfterInsertNewSchemaDF = spark.sql(s"SELECT * FROM $TargetTable")
    companiesHiveAfterInsertNewSchemaDF.printSchema()
    companiesHiveAfterInsertNewSchemaDF.show(false)

    val fourthCompaniesDF = Seq("G", "H")
      .toDF("id")

    val mergedFlatDF =
      fourthCompaniesDF.transform(DataFrameSchemaUtils.mergeDataFrameSchemaAgainstTable(thirdCompaniesDF))
    mergedFlatDF.write.mode(SaveMode.Overwrite).insertInto(TargetTable)
    mergedFlatDF.printSchema()
    mergedFlatDF.show(false)

  }

}
