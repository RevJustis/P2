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
    var continue = true
    val op = List[String](
      "Topic 1",
      "Topic 2",
      "Topic 3",
      "Topic 4",
      "End Program"
    )
    val list1 = List[String]("A", b)
    val list2 = List[String]("B", "C", "D", b)
    val list3 = List[String]("E", "F", "Unknown", "PEDAL", b)
    val list4 = List[String]("G", "H", b)

    val menu = new MyMenu(op)
    var continue = true

//    spark.sql("select * from personskilled").show()

    spark
      .sql(
        "select year, passengerCars, buses, total1 as TotalExcludingMotorcyclesAndPed, " +
          "motorcycles as Delta, (total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as DeltaPED," +
          " total from personsKilled where year between 2008 and 2018"
      )
      .show()

//    spark.sql("select year, buses, total1 as totalNotIncludingMotorcycles, (total1 + motorcycles)" +
//      "as TotalNotIncludingPed from personsKilled where year between 2008 and 2018").show()

//    spark.read
//      .option("header", true)
//      .csv("input/main/*")
//      .toDF()

    /*case class data2016(crashtype: Int, age15to19: Int, age15to20: Int, age16to19: Int, age16to20: Int, age16to24: Int,
                      age21to24: Int, older65: Int, InvolvingLGTRK: Int, InvolvingMoto: Int, InvolvingPed: Int,
                      InvolvingPedal: Int, InvolvingPedalF: Int, InvolvingPedF: Int, InvolvingRdDep: Int,
                      RelationToRd: Int, Fatalities: Int, schoolBusRelated: Int, State: Int, StateName: String,
                      StCase: Int, year: Int)*/

    /* val df16 = spark.read
      .option("header",true)
      .csv("input/PersonsKilled/2016.csv")
      .where("A_PED_F ==1")
    df16.show()

    val sum = df16.groupBy("STATENAME").agg(functions.sum("A_LT")).as("LGTruckSum").show()*/

    /*val rdd16= spark.sparkContext.textFile("input/PersonsKilled/2016.csv")
    import spark.implicits._
    val ds16 = rdd16.toDS()
    ds16.show()*/

    /*println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true
>>>>>>> Q4

    while (continue) {
<<<<<<< HEAD
      getOption(op) match {
        case "Topic 1"     => qMenu(list1)
        case "Topic 2"     => qMenu(list2)
        case "Topic 3"     => qMenu(list3)
        case "Topic 4"     => qMenu(list4)
||||||| 237ab95
      menu.printMenu()
      // print("Option: ")
      val in = chooseN(7)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List[String]("A"), 1)
        case "Topic 2"     => menuLev2(List[String]("B", "C", "D"), 3)
        case "Topic 3"     => menuLev2(List[String]("E", "F"), 2)
        case "Topic 4"     => menuLev2(List[String]("G", "H"), 2)
=======
      menu.printMenu()
      // print("Option: ")
      val in = chooseN(7)
      val option = menu.selectOption(in)

      option match {
        case "Topic 1"     => menuLev2(List[String]("A", b), 2)
        case "Topic 2"     => menuLev2(List[String]("B", "C", "D", b), 4)
        case "Topic 3"     => menuLev2(List[String]("E", "F", b), 3)
        case "Topic 4"     => menuLev2(List[String]("G", "H", b), 3)
>>>>>>> Q4
        case "End Program" => continue = false
      }
    }
    spark.close
    end*/
  }
}
