package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exo2 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample7DF = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_07")
      .select($"_c0".as("code"), $"_c1".as("description"), $"_c2".as("total_emp"), $"_c3".as("salary"))

    val sample8DF = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"), $"_c1".as("description"), $"_c2".as("total_emp"), $"_c3".as("salary"))


    sample7DF
      .select("description", "salary")
      .where($"salary".>=(100000))
      .orderBy($"salary".desc)
      .show(20)

    val joinedDF = sample7DF
      .select($"description", $"salary".as("2007_salary"), $"total_emp".as("2007_total_emp"))
      .join(sample8DF.select($"description", $"salary".as("2008_salary"), $"total_emp".as("2008_total_emp")), Seq("description"))

    joinedDF
      .select($"description", $"2007_salary", $"2008_salary", (($"2008_salary" - $"2007_salary") / $"2007_salary" * 100).as("growth"))
      .where($"growth" > 0)
      .orderBy($"growth".desc)
      .show(20)


    joinedDF
      .select($"description", $"2007_total_emp", $"2008_total_emp", (($"2008_total_emp" - $"2007_total_emp") / $"2007_total_emp" * 100).as("loss"))
      .where($"loss" < 0)
      .orderBy($"loss".asc)
      .show(20)
  }


}
