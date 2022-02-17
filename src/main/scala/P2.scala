import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Utilities._

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

    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true

    while (continue) {
      menu.printMenu()
      val in = chooseN(5)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List[String]("A", b), 1)
        case "Topic 2"     => menuLev2(List[String]("B", "C", "D", b), 3)
        case "Topic 3"     => menuLev2(List[String]("E", "F", "Sub", b), 2)
        case "Topic 4"     => menuLev2(List[String]("G", "H", b), 2)
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}
