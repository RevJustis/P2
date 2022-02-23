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

  val main = spark.read
    .option("header", true)
    .csv("input/main/*")
  main.write.mode("overwrite").parquet("input/mainPF.parquet")
  val mainPF = spark.read.parquet("input/mainPF.parquet")
  mainPF.persist(StorageLevel.MEMORY_ONLY_SER)

  val t1q1 = "Pedestrian Totals"
  val t1q2 = "Pedestrian Fatal Totals"
  val t1q3 = "Pedestrian By State"
  val t1q4 = "Pedestrian By Sex"
  val t1q5 = "Pedestrian By Age"

  val t2q1 = "US Wide Fatalities"
  val t2q2 = "State Wide Fatalities"
  val t2q3 = "Most Fatal States"

  val t3q1 = "Rural"
  val t3q2 = "Urban"
  val t3q3 = "Other"

  val t4q1 = "Fatalities by Vehicle"
  val t4q2 = "Vehicle Types"
  val t4q3 = "Cyclists"
  val t2q4 = "Least Fatal States"

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    prep
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

    val list1 = List[String](t1q1, t1q2, t1q3, t1q4, t1q5, b)
    val list2 = List[String](t2q1, t2q2, t2q3, t2q4, b)
    val list3 = List[String](t3q1, t3q2, t3q3, b)
    val list4 = List[String](t4q1, t4q2, t4q3, b)

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
