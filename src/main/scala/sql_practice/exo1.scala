package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exo1 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demographieDF = spark.read
      .json("data/input/demographie_par_commune.json")


    demographieDF
      .select($"Population")
      .agg(sum("Population").as("Population totale"))
      .show

    demographieDF
      .select("Departement", "Population")
      .groupBy("Departement")
      .agg(sum("Population").as("Population totale"))
      .orderBy($"Population totale".desc)
      .show(20)

    val groupDepDF =
      demographieDF
        .select("Departement", "Population")
        .groupBy("Departement")
        .agg(sum("Population").as("Population totale"))
        .orderBy($"Population totale".desc)

    var departementDF = spark.read
      .csv("data/input/departements.txt")
      .select($"_c0".as("Departement name"), $"_c1".as("Departement"))

    var joinedDF = groupDepDF
      .join(departementDF, Seq("Departement"))

    joinedDF.show(20)

  }


}
