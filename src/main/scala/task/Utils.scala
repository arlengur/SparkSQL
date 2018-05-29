package task

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object Utils {
  def colData(df: DataFrame, colName: String, rowCount: Int)(implicit spark: SparkSession): Profiling = {
    val tempVal = df
                  .where(df(colName).isNotNull)
                  .groupBy(colName)
                  .agg(count(colName))
    import spark.implicits._
    val values = tempVal
                 .as[(String, Long)]
      .take(rowCount)
      .map { case (name, count) => Map(name -> count) }
    val unique = tempVal
                 .select(count(tempVal(colName)))
                 .as[Long]
      .first()
    Profiling(colName, unique, values)
  }

  def parse: UserDefinedFunction = udf { x: String =>
    Try(x.trim.replaceAll("‘|’", "")).toOption
  }

  def removeQuote(s: String) = {
    s.replaceAll("‘|’", "").trim
  }
}
