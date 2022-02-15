import org.apache.spark.sql.SparkSession
import Utilities._

object P2 {
  def main(args: Array[String]): Unit = {
    val op = List[String](
    "Scenario 1",
    "Scenario 2",
    "Scenario 3",
    "Scenario 4",
    "Scenario 5",
    "Scenario 6",
    "End Program"
  )
    val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true

    while (continue) {
      menu.printMenu()
      print("Option: ")
      val in = chooseN(7)
      val option = menu.selectOption(in)

      option match {
        case "Scenario 1" => println(option)
        case "Scenario 2" => println(option)
        case "Scenario 3" =>println(option)
        case "Scenario 4" => println(option)
        case "Scenario 5" => println(option)
        case "Scenario 6" =>println(option)
          // val df = spark.sql("SELECT * FROM table")
          // df.show
          // df.coalesce(1).write.format("csv").option("header",true).mode("overwrite").save("hdfs://localhost:9000/user/justis/future.csv")
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}
