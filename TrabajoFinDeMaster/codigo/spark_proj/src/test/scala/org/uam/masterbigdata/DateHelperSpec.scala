package org.uam.masterbigdata

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.scalatest.funspec.AnyFunSpec
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.types._
import java.sql.Timestamp

class DateHelperSpec extends AnyFunSpec with SparkSessionTestWrapper with ColumnComparer{
  import spark.implicits._

 describe("convertToDate"){
   it("Convierte una cadena de texto en formato de fecha común"){

     val df = Seq(
       ("2023-02-05T17:25:03Z", new Timestamp(1675614303000L))
     ).toDF("date", "expected")
     val rest = df.transform(DateHelper.convertToDate("date", "current"))

     assertColumnEquality(rest, "expected", "current")
   }
   it("Convierte una cadena de texto con milisegundos en formato de fecha común") {

     val df = Seq(
       ("2023-02-05T13:15:18.01Z", new Timestamp(1675599318010L))
     ).toDF("date", "expected")
     val rest = df.transform(DateHelper.convertToDate("date", "current"))
     assertColumnEquality(rest, "expected", "current")
   }
 }
}
