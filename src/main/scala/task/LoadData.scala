package task

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import task.Utils._

trait DataProcessing {
  def sparkInit(): SparkSession

  def loadData(path: String)(implicit spark: SparkSession): sql.DataFrame

  def filterEmptyCells(df: sql.DataFrame): sql.DataFrame

  def loadUserInput(path: String)(implicit spark: SparkSession): sql.DataFrame

  def transformData(userList: Array[Input],
                    newCols: Array[String],
                    oldCols: Array[String],
                    df: sql.DataFrame): sql.DataFrame

  def profiling(df: sql.DataFrame, rowCount: Int)(implicit spark: SparkSession): Array[Profiling]
}

object Test extends DataProcessing {
  def main(args: Array[String]): Unit = {
    // Spark init
    implicit val spark = sparkInit()
    import spark.implicits._

    // Step 1
    val sample1 = loadData("data/Sample.csv")
    println("Step 1:")
    sample1.show()

    // Step 2
    val sample2 = filterEmptyCells(sample1)
    println("Step 2:")
    sample2.show()

    // Step 3
    val df = loadUserInput("data/user_input.json")
    val userList = df.as[Input].collect
    val newColumns = userList.map(_.new_col_name)
    val oldColumns = sample2.columns
    val sample3 = transformData(userList, newColumns, oldColumns, sample2)
    println("Step 3:")
    sample3.show()

    // Step 4
    val prof = profiling(sample3, 10)
    implicit val formats = DefaultFormats
    import org.json4s.native.Serialization.writePretty
    val profilingJson = writePretty(prof)
    println(profilingJson)
  }

  override def sparkInit(): SparkSession = {
    System.setProperty("hadoop.home.dir", "C:\\repo\\BigData\\lib")
    val spark = SparkSession.builder()
                .master("local[*]")
                .appName("LoadData")
                .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  override def loadData(path: String)(implicit spark: SparkSession): DataFrame = {
    val sample = spark.read.option("header", "true").csv("data/Sample.csv")
    val columns = sample.columns.toSeq.map(x => removeQuote(x))
    sample.toDF(columns: _*)
  }

  override def filterEmptyCells(df: DataFrame): DataFrame = {
    df.filter(!_.toSeq.exists(x => x != null && removeQuote(x.toString).isEmpty))
  }

  override def loadUserInput(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.json("data/user_input.json")
  }

  override def transformData(userList: Array[Input],
                             newCols: Array[String],
                             oldCols: Array[String],
                             df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    userList.foldLeft(df) {
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
    }.drop(oldCols.diff(newCols): _*)
  }

  override def profiling(df: DataFrame, rowCount: Int)(implicit spark: SparkSession): Array[Profiling] = {
    df.columns.map(c => colData(df, c, rowCount))
  }
}
