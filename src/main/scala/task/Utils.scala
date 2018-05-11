package task

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.count
import task.LoadData.sample3

import scala.util.{Failure, Success, Try}

object Utils {
  def colData(df: DataFrame, colName: String): Profiling = {
    val tempVal = df
                  .where(df(colName).isNotNull)
                  .groupBy(colName)
                  .agg(count(colName))
    val values = tempVal
                 .collect()
                 .map(r => Map(r.get(0).toString -> r.getLong(1)))
    val unique = tempVal
                 .select(count(tempVal(colName)))
                 .first()
                 .getLong(0)
    Profiling(colName, unique, values)
  }

  import org.apache.spark.sql.functions._

  def parse: UserDefinedFunction = udf { x: String =>
    Try(x.trim.replaceAll("‘|’", "")) match {
      case Success(date) => Some(date)
      case Failure(ex) => None
    }
  }

}
