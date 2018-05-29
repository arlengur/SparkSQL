package task

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{concat_ws, count, lit}

import scala.util.Try

object Utils {
  def colData(df: DataFrame, colName: String, rowCount: Int): Profiling = {
    val tempVal = df
                  .where(df(colName).isNotNull)
                  .groupBy(colName)
                  .agg(count(colName))
    import org.apache.spark.sql.functions._
    val values = tempVal
                 .take(rowCount)
                 .map(r => Map(r.get(0).toString -> r.getLong(1)))
    val unique = tempVal
                 .select(count(tempVal(colName)))
                 .first()
                 .getLong(0)
    Profiling(colName, unique, values)
  }

  import org.apache.spark.sql.functions._

  def parse: UserDefinedFunction = udf { x: String =>
    Try(x.trim.replaceAll("‘|’", "")).toOption
  }

  def removeQuote(s: String) = {
    s.replaceAll("‘|’", "").trim
  }
}
