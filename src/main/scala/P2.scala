import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Utilities._

object P2 {
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

    println("TRANSPORTATION AND AUTO DATA (F.I.R.S.T.)")
    val menu = new MyMenu(op)
    var continue = true
    val list1 = List[String]("A", b)
    val list2 = List[String]("B", "C", "D", b)
    val list3 = List[String]("E", "F", "Unknown", "PEDAL", b)
    val list4 = List[String]("G", "H", b)

    while (continue) {
      menu.printMenu()
      val in = chooseN(5)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(list1)
        case "Topic 2"     => menuLev2(list2)
        case "Topic 3"     => menuLev2(list3)
        case "Topic 4"     => menuLev2(list4)
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}
