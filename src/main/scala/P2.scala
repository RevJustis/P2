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
      val s1 = "Go to Main Menu"
      val s2 = "Make new Admin"
      val s3 = "Refresh All"
      val s4 = "End Program"
      getOption(List[String](s1, s2, s3, s4)) match {
        case `s2` =>
          println(
            "Please enter username of the account you want to make admin:"
          )
          var continue = false
          while (!continue) {
            val in = readLine.trim
            if (userExists(in)) {
              makeAdmin(in)
              continue = true
            } else {
              println("Sorry, that username doesn't exist!")
            }
          }
        case `s3` => prep
        case `s4` => System.exit(0)
        case `s1` => // do nothing
      }
    }

    val s1 = "Topic 1"
    val s2 = "Topic 2"
    val s3 = "Topic 3"
    val s4 = "Topic 4"
    val s5 = "End Program"
    // TODO update these lists!
    val list1 = List[String]("A", b)
    val list2 = List[String]("USfatals", "StateFatals", "Safest", b)
    val list3 = List[String]("E", "F", "Unknown", "PEDAL", b)
    val list4 = List[String]("G", "H", b)

    var continue = false
    while (!continue) {
      getOption(List[String](s1, s2, s3, s4, s5)) match {
        case `s1` => qMenu(list1)
        case `s2` => qMenu(list2)
        case `s3` => qMenu(list3)
        case `s4` => qMenu(list4)
        case `s5` => continue = true
      }
    }
    spark.close
  }
}
