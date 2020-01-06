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


    print("\n French population: \n")
    demoDF
      .agg(sum($"population"))
      .show

    print("\n Population by department, ordered by population : \n")
    demoDF
      .groupBy($"departement")
      .agg(sum($"population"))
      .orderBy(sum($"population").desc)
      .show(20)

    print("\n Same but with the names of the departments : \n")
    demoDF
      .groupBy($"departement")
      .agg(sum($"population"))
      .orderBy(sum($"population").desc)
      .join(departement.select($"_c0".alias("nomDepartement"),$"_c1"
        .alias("numDepartement")),$"departement"===$"numDepartement")
      .select($"numDepartement",$"nomDepartement",$"sum(population)")
      .show(20)


  }


  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample07 = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .option("delimiter","\t")
      .csv("data/input/sample_07")


    val sample08 = spark.read
      .option("multiline", false)
      .option("mode", "PERMISSIVE")
      .option("delimiter","\t")
      .csv("data/input/sample_08")


    print("\n Salaries over 100k in 2007 : \n")
    sample07
      .select($"_c1".alias("occupation"),$"_c3".alias("salary").cast("int"))
      .filter($"salary" > 100000)
      .orderBy($"salary".desc)
      .show(20)

    print("Greatest salary growths : \n")
    sample07
      .select($"_c1".alias("occupation"),$"_c3".alias("salary7").cast("int"),$"_c0".alias("code7"))
      .join(sample08.select($"_c0".alias("code8"),$"_c3"
        .alias("salary8").cast("int")),$"code8"===$"code7")
      .select($"occupation",($"salary8"-$"salary7").alias("salary growth").cast("int"))
      .orderBy($"salary growth".desc)
      .show(20)

print("Job loss in the greatest salary growths : \n")
    sample07
      .select($"_c1".alias("occupation"),$"_c2".alias("emp7").cast("int"),$"_c3".alias("salary7").cast("int"),$"_c0".alias("code7"))
      .join(sample08.select($"_c0".alias("code8"),$"_c2".alias("emp8").cast("int"),$"_c3"
        .alias("salary8").cast("int")),$"code8"===$"code7")
      .select($"occupation",($"salary8"-$"salary7").alias("salary growth").cast("int"),($"emp7"-$"emp8").alias("job loss").cast("int"))
      .orderBy($"salary growth".desc)
      .show(20)
  }

}



