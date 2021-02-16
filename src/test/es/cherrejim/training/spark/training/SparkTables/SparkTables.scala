package es.cherrejim.training.spark.training.SparkTables

import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, VarcharType}
import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import org.scalatest.matchers.must.Matchers

class SparkTables extends SharedSparkSessionHelper with Matchers {

  it should "sdsad" in {

    val path           = getClass.getResource("/events")
    val out_path       = getClass.getResource("/out/")
    val _df: DataFrame = spark.read.parquet(path.toString)

    import org.apache.spark.sql.functions.lit
    val df = _df.withColumn("dt", lit("2019114")).withColumn("version", lit("1.6"))

    def renameAllCols(schema: StructType, rename: String => String): StructType = {
      def recurRename(schema: StructType): Seq[StructField] = schema.fields.map {
        case StructField(name, dtype: StructType, nullable, meta) => {
          StructField(rename(name), StructType(recurRename(dtype)), nullable, meta)
        }
        case StructField(name, dtype: ArrayType, nullable, meta) if dtype.elementType.isInstanceOf[StructType] => {
          StructField(rename(name),
                      ArrayType(StructType(recurRename(dtype.elementType.asInstanceOf[StructType])), true),
                      nullable,
                      meta)
        }

        /* case StructField(name, dtype: StringType, nullable, meta) => {
          StructField(rename(name), VarcharType(1024), nullable, meta)
        }*/

        case StructField(name, dtype, nullable, meta) => {
          StructField(rename(name), dtype, nullable, meta)
        }
      }
      StructType(recurRename(schema))
    }

    val renameFc = (s: String) => s.replace("@", "_")

    val newDF = spark.createDataFrame(df.rdd, renameAllCols(df.schema, renameFc))

    spark.sql("DROP TABLE IF EXISTS TOMATE")
    newDF.write
      .option("path", "/tmp/trainig-spark/out/")
      .mode("overwrite")
      .partitionBy("version", "dt")
      .saveAsTable("tomate")

    newDF.printSchema()

    spark.sql("SHOW CREATE TABLE TOMATE").show(false)

  }

  it should "aaaaaaa" in {

    val path           = getClass.getResource("/events")
    val out_path       = getClass.getResource("/out/")
    val _df: DataFrame = spark.read.parquet(path.toString)

    import org.apache.spark.sql.functions.lit
    val df = _df.withColumn("dt", lit("2019114")).withColumn("version", lit("1.6"))

    df.write
      .option("path", "/tmp/trainig-spark/out2/")
      .mode("overwrite")
      .partitionBy("version", "dt")
      .saveAsTable("tomate2")

    spark.sql(
      """
        |CREATE TABLE `tomate3` (`actor` STRUCT<`@id`: STRING, `remoteAddress`: STRING, `remoteUserAgent`: STRING>, `provider` STRUCT<`@id`: STRING, `product`: STRING, `channel`: STRING>, `published` STRING, `schema` STRING, `normalizationEventObject` STRUCT<`@id`: STRING, `normalizedValue`: STRING, `normalizationScore`: DOUBLE, `normalizationModel`: STRING, `valueId`: STRING, `value`: STRING, `processedValue`: STRING, `type`: STRING, `tenant`: STRING, `createdAt`: STRING, `updatedAt`: STRING>, `@type` STRING, `action` STRING, `@id` STRING, `type` STRING, `providerId` STRING, `version` STRING, `dt` STRING)
        |USING parquet
        |OPTIONS (
        |  `serialization.format` '1',
        |  path 'file:/tmp/trainig-spark/out2'
        |)
        |PARTITIONED BY (version, dt)
        |""".stripMargin
    )

    spark.sql("show create table tomate3").show(false)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `TOMATE2` (`actor` STRUCT<`_id`: VARCHAR(1024), `remoteAddress`: VARCHAR(1024), `remoteUserAgent`: VARCHAR(1024)>, `provider` STRUCT<`_id`: VARCHAR(1024), `product`: VARCHAR(1024), `channel`: VARCHAR(1024)>, `published` VARCHAR(1024), `schema` VARCHAR(1024), `normalizationEventObject` STRUCT<`_id`: VARCHAR(1024), `normalizedValue`: VARCHAR(1024), `normalizationScore`: DOUBLE, `normalizationModel`: VARCHAR(1024), `valueId`: VARCHAR(1024), `value`: VARCHAR(1024), `processedValue`: VARCHAR(1024), `type`: VARCHAR(1024), `tenant`: VARCHAR(1024), `createdAt`: VARCHAR(1024), `updatedAt`: VARCHAR(1024)>, `_type` VARCHAR(1024), `action` VARCHAR(1024), `_id` VARCHAR(1024), `type` VARCHAR(1024), `providerId` VARCHAR(1024))
        |USING parquet
        |OPTIONS (
        |  `serialization.format` '1',
        |  path '/tmp/trainig-spark/out/'
        |)""".stripMargin
    )

    def renameAllCols(schema: StructType, rename: String => String): StructType = {
      def recurRename(schema: StructType): Seq[StructField] = schema.fields.map {
        case StructField(name, dtype: StructType, nullable, meta) => {
          StructField(rename(name), StructType(recurRename(dtype)), nullable, meta)
        }
        case StructField(name, dtype: ArrayType, nullable, meta) if dtype.elementType.isInstanceOf[StructType] => {
          StructField(rename(name),
                      ArrayType(StructType(recurRename(dtype.elementType.asInstanceOf[StructType])), true),
                      nullable,
                      meta)
        }
        case StructField(name, dtype, nullable, meta) => {
          StructField(rename(name), dtype, nullable, meta)
        }
      }
      StructType(recurRename(schema))
    }
    /*
    val renameFc = (s: String) => s.replace("@", "_")

    val newDF = spark.createDataFrame(df.rdd, renameAllCols(df.schema, renameFc))

    import org.apache.spark.sql.functions.col

    newDF.write
      .option("path", "/tmp/trainig-spark/out2/")
      .mode("overwrite")
      .partitionBy("version", "dt")
      .saveAsTable("tomate2")

    newDF.printSchema()

    spark.sql("SHOW CREATE TABLE TOMATE2").show(false)
   */
  }

}
