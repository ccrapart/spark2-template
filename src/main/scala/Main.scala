import spark_helpers.SessionBuilder

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.buildSession()

    val sparkVersion = spark.version
    println(s"Spark Version: $sparkVersion")

    sql_practice.exo1.exec1()
  }
}
