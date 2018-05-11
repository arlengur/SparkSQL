package task

case class Input(existing_col_name: String, new_col_name: String, new_data_type: String, date_expression: String)
case class Profiling(Column: String, Unique_values: Long, Values: Array[Map[String, Long]])
