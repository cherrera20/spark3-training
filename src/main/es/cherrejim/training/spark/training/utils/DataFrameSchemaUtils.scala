package es.cherrejim.training.spark.training.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object DataFrameSchemaUtils {

  def getMissingTableColumnsAgainstDataFrameSchema(df: DataFrame, tableDF: DataFrame): Set[String] = {
    val dfSchema    = df.schema.fields.map(v => (v.name, v.dataType)).toMap
    val tableSchema = tableDF.schema.fields.map(v => (v.name, v.dataType)).toMap
    val columnsMissingInTable =
      dfSchema.keys.toSet.diff(tableSchema.keys.toSet).map(x => x.concat(s" ${dfSchema.get(x).get.sql}"))

    columnsMissingInTable
  }

  def mergeDataFrameSchemaAgainstTable(tableDF: DataFrame)(df: DataFrame): DataFrame = {
    val dfSchema    = df.schema.fields.map(v => (v.name, v.dataType)).toMap
    val tableSchema = tableDF.schema.fields.map(v => (v.name, v.dataType)).toMap

    val columnMissingInDF = tableSchema.keys.toSet.diff(dfSchema.keys.toSet).toList

    val mergedDFWithNewColumns = columnMissingInDF.foldLeft(df) { (currentDF, colName) =>
      currentDF.withColumn(
        colName,
        lit(null).cast(tableSchema.get(colName).get.typeName)
      )
    }

    mergedDFWithNewColumns
  }

}
