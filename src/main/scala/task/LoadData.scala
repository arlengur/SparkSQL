package task

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import task.Schema._
import task.Utils._

object LoadData extends App {
  System.setProperty("hadoop.home.dir", "C:\\repo\\BigData\\lib")
  val spark = SparkSession.builder()
              .master("local[*]")
              .appName("LoadData")
              .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  // Step 1

  val sample1 = spark.read
                .option("header", "true")
                .schema(schema)
                .csv("data/Sample.csv")

  sample1.show()

  // Step 2

  val sample2 = sample1.filter(!_.toSeq.exists(x => x != null && x.toString.replaceAll("‘|’", "").trim.isEmpty))
  sample2.show()

  // Step 3

  val df = spark.read
           .schema(user_schema)
           .json("data/user_input.json")

  val userList = df.as[Input].collect
  val newColumns = userList.map(_.new_col_name)
  val oldColumns = sample2.columns

  import org.apache.spark.sql.functions._
  val sample3 = userList.foldLeft(sample2) {
    (acc, userList) =>
      userList.new_data_type match {
        case "date" =>
          acc.withColumn(userList.new_col_name, to_date(parse(acc(userList.existing_col_name)), userList.date_expression))
          .drop(userList.existing_col_name)
        case s: String =>
          acc.withColumn(userList.new_col_name, parse(acc(userList.existing_col_name)).cast(s))
          .drop(userList.existing_col_name)
        case _ => acc
      }
  }.drop(oldColumns.diff(newColumns): _*)

  sample3.show()

  // Step 4
  val profiling = sample3.columns.map(c => colData(sample3, c))

  implicit val formats = DefaultFormats
  val profilingJSON = write(profiling)
  println(profilingJSON)

}
