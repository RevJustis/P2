import Utilities._
import org.apache.spark.sql.SparkSession

object P2 {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  val b = "Back to Main Menu"

  def main(args: Array[String]): Unit = {
    val op = List[String](
      "Topic 1",
      "Topic 2",
      "Topic 3",
      "Topic 4",
      "End Program"
    )



    /*spark.sql(
      "CREATE TABLE IF NOT EXISTS test (year STRING, total STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/FileName.txt' OVERWRITE INTO TABLE test"
    )
    spark.sql("select * from test").show*/

    spark.sql(
      "CREATE TABLE IF NOT EXISTS personsKilled (year int, passengerCars int, lightTrucks int, largeTrucks int," +
        "motorcycles int, buses int, otherUnknown int, total1 int, pedestrian int, pedalcyclist int, other int, total2 int," +
        "unknownPersonType int, total int)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/PersonsKilled/PersonsKilled.csv' OVERWRITE INTO TABLE personsKilled")

//    spark.sql("select * from personskilled").show()

    spark.sql("select year, passengerCars, total1 + total2 as totalNotIncludingPed from personsKilled where year between 2008 and 2018 ").show()
    spark.sql("select year, buses, total from personsKilled where year between 2008 and 2018").show()

//    spark.read
//      .option("header", true)
//      .csv("input/main/*")
//      .toDF()






    /*println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true

    while (continue) {
      menu.printMenu()
      // print("Option: ")
      val in = chooseN(7)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List[String]("A", b), 2)
        case "Topic 2"     => menuLev2(List[String]("B", "C", "D", b), 4)
        case "Topic 3"     => menuLev2(List[String]("E", "F", b), 3)
        case "Topic 4"     => menuLev2(List[String]("G", "H", b), 3)
        case "End Program" => continue = false
      }
    }
    spark.close
    end*/
  }
}
