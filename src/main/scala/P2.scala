import org.apache.spark.sql.SparkSession
import Utilities._

object P2 {
  def main(args: Array[String]): Unit = {
    val op = List[String](
      "Topic 1",
      "Topic 2",
      "Topic 3",
      "Topic 4",
      "End Program"
    )
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(
      "CREATE TABLE IF NOT EXISTS test (year STRING, total STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/FileName.txt' OVERWRITE INTO TABLE test"
    )
    spark.sql("select * from test").show
    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true

    while (continue) {
      menu.printMenu()
      // print("Option: ")
      val in = chooseN(7)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List[String]("A"), 1)
        case "Topic 2"     => menuLev2(List[String]("B", "C", "D"), 3)
        case "Topic 3"     => menuLev2(List[String]("E", "F"), 2)
        case "Topic 4"     => menuLev2(List[String]("G", "H"), 2)
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}
