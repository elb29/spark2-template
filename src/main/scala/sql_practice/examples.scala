package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoDF = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .json("/home/etienne/Bureau/Sample Data/Spark/data/demographie_par_commune.json")
    //demoDF.show

    val departement = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .csv("/home/etienne/Bureau/Sample Data/Spark/data/departements.txt")


    demoDF
      .agg(sum($"population"))
      .show

    demoDF
      .groupBy($"departement")
      .agg(sum($"population"))
      .orderBy(sum($"population").desc)
      .show(20)

    demoDF
      .groupBy($"departement")
      .agg(sum($"population"))
      .orderBy(sum($"population").desc)
      .join(departement.select($"_c0".alias("nomDepartement"),$"_c1"
        .alias("numDepartement")),$"departement"===$"numDepartement")
      .select($"numDepartement",$"nomDepartement",$"sum(population)")
      .show(20)


  }




}



