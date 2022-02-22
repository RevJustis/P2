import Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

object P2 {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()
  val sc = spark.sparkContext
  val b = "Back to Main Menu"

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    prep
    prep()
    var auth = false
    while (!auth) {
      getOption(List[String]("Log In", "Sign Up", "Quit Program")) match {
        case "Sign Up" =>
          logIn(signUp())
          auth = true
        case "Log In" =>
          println("Please enter your Username")
          var user = readLine()
          var break = false
          while (!userExists(user) && !break) {
            println("Sorry, that username doesn't match.")
            getOption(List[String]("Try Log In again", "Quit Log In")) match {
              case "Try Log In again" =>
                println("Please enter your Username")
                user = readLine()
              case "Quit Log In" => break = true
            }
          }
          if (!break) {
            var continue = false
            while (!continue) {
              println("Please enter your Password")
              var pass = readLine()
              if (authPass(user, pass)) {
                continue = true
                logIn(user)
                auth = true
              } else {
                println("Sorry, your password is incorrect")
                getOption(List[String]("Try Again", "Quit")) match {
                  case "Try Again" => //do nothing
                  case "Quit"      => continue = true
                }
              }
            }
          }
        case "Quit Program" => System.exit(0)
      }
    }

    if (admin) {
      val op = List[String](
        "Go to Main Menu",
        "Make new Admin",
        "Refresh All",
        "End Program"
      )
      getOption(op) match {
        case "Go to Main Menu"            => // do nothing
        case "Make another user an Admin" => println("Comming Soon!")
        case "Refresh All"                => prep
        case "End Program"                => System.exit(0)
      }
    }

    val op = List[String](
      "Topic 1",
      "Topic 2",
      "Topic 3",
      "Topic 4",
      "End Program"
    )
    val list1 = List[String]("A", b)
    val list2 = List[String]("USfatals", "StateFatals", "Safest", b)
    val list3 = List[String]("E", "F", "Unknown", "PEDAL", b)
    val list4 = List[String]("G", "H", b)

    var continue = false
    while (!continue) {
      getOption(op) match {
        case "Topic 1"     => qMenu(list1)
        case "Topic 2"     => qMenu(list2)
        case "Topic 3"     => qMenu(list3)
        case "Topic 4"     => qMenu(list4)
        case "End Program" => continue = true
      }
    }
    spark.close
  }
}
