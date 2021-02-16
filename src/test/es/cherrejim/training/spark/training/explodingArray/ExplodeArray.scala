package es.cherrejim.training.spark.training.explodingArray

import es.cherrejim.training.spark.training.SharedSparkSessionHelper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, explode, explode_outer, lit}
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable.ListBuffer

class ExplodeArray extends SharedSparkSessionHelper with Matchers {

  it should "bla bla bvla bla" in {

    import testImplicits._

    /*
    val _df = Seq(
      ("id1", "t1", Array(("n1", 4L), ("n2", 5L))),
      ("id2", "t2", Array(("n3", 6L), ("n4", 7L)))
    ).toDF("ID", "Time", "Items")

     */

    val initSchema = StructType(
      List(
        StructField("id", IntegerType, nullable = true),
        StructField("offer",
                    StructType(
                      List(
                        StructField("offerid", ArrayType(StringType))
                      )))
      )
    )

    val someData = Seq(
      Row(1, Row(Array("hola", "adios"))),
      Row(1, Row(Array("sol", "luna"))),
      Row(1, Row(Array("perro", "gato")))
    )

    val _df = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(initSchema)
    )

    import scala.collection.mutable.ListBuffer

    def pepe(schema: StructType,
             prefix: String = "",
             listaColumnas: ListBuffer[String] = new ListBuffer()): List[String] = {

      schema.fields
        .map(
          f => {
            f match {
              case StructField(name, ArrayType(_, _), _, _) => {
                listaColumnas += prefix.concat(s".$name")
              }

              case StructField(name, str: StructType, _, _) => {
                pepe(str, name, listaColumnas)
              }

              case _ => listaColumnas
            }
          }
        )
      listaColumnas.toList
    }

    val tomate = pepe(_df.schema)
    println(tomate)

    _df.printSchema()
    _df.show(false)

    val df2 = tomate.foldLeft(_df) { (memoDF, colName) =>
      val colEliminar: String = colName.split("\\.").dropRight(1).mkString(".")

      memoDF
        .withColumn(
          colName,
          explode(col(colName))
        )
        .drop(col(colEliminar))
    }

    df2.printSchema()
    df2.show(false)

  }
}
