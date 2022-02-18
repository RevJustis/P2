import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Utilities._
import org.apache.spark.storage.StorageLevel

object P2 {
  //create a spark session
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()

  val b = "Back to Main Menu"

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    val op = List[String](
      "Topic 1",
      "Topic 2",
      "Topic 3",
      "Topic 4",
      "End Program"
    )




    //---------------------------------------------------------------------------------------------------------------


    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true
    val List1 = List[String]("A", b)
    val List2 = List[String]("B", "C", "D", b)
    val List3 = List[String]("E", "F", b)
    val List4 = List[String]("G", "H", b)

    while (continue) {
      menu.printMenu()
      // print("Option: ")
      val in = chooseN(5)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List1)
        case "Topic 2"     => menuLev2(List2)
        case "Topic 3"     => menuLev2(List3)
        case "Topic 4"     => menuLev2(List4)
        case "End Program" => continue = false
      }
    }

    //Close Spark Session
    spark.close
    end
  }
}
