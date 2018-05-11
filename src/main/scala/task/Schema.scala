package task

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schema {
  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("age", StringType),
    StructField("birthday", StringType),
    StructField("gender", StringType)
  ))

  val user_schema = StructType(Array(
    StructField("existing_col_name", StringType),
    StructField("new_col_name", StringType),
    StructField("new_data_type", StringType),
    StructField("date_expression", StringType)
  ))

}
