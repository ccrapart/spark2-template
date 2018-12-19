package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exo3 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiLine", true)
      .json("data/input/tours.json")
      .cache()

    toursDF.show(20)

    toursDF
      .select($"tourDifficulty")
      .distinct()
      .show

    toursDF
      .select($"tourPrice")
      .agg(min($"tourPrice").as("Minimum Price"), max($"tourPrice").as("Maximum Price"), avg($"tourPrice").as("Average Price"))
      .show

    toursDF
      .select($"tourDifficulty", $"tourPrice")
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("Minimum Price"), max($"tourPrice").as("Maximum Price"), avg($"tourPrice").as("Average Price"))
      .show

    toursDF
      .select($"tourDifficulty", $"tourLength", $"tourPrice")
      .groupBy($"tourDifficulty")
      .agg(
        min($"tourPrice").as("Minimum Price"),
        max($"tourPrice").as("Maximum Price"),
        avg($"tourPrice").as("Average Price"),
        min($"tourLength").as("Minimum Length"),
        max($"tourLength").as("Maximum Length"),
        avg($"tourLength").as("Average Length")
      )
      .show

    toursDF
      .select(explode($"tourTags").as("Tags"))
      .groupBy("Tags")
      .agg(count("Tags").as("Count"))
      .orderBy($"Count".desc)
      .show(10)

    toursDF
      .select($"tourDifficulty", explode($"tourTags").as("Tags"))
      .groupBy($"tourDifficulty", $"Tags")
      .agg(count("Tags").as("Count"))
      .orderBy($"Count".desc)
      .show(10)

    toursDF
      .select($"tourDifficulty", $"tourPrice", explode($"tourTags").as("Tags"))
      .groupBy($"tourDifficulty", $"Tags")
      .agg(
        min($"tourPrice").as("Minimum Price"),
        max($"tourPrice").as("Maximum Price"),
        avg($"tourPrice").as("Average Price")
      )
      .orderBy($"Average Price".desc)
      .show(10)

  }


}
