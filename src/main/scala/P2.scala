import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Utilities._
import org.apache.spark.storage.StorageLevel

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
                val option = getOption(List[String]("Try Again", "Quit"))
                option match {
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
        "Do Admin things",
        "End Program"
      )
      getOption(op) match {
        case "Go to Main Menu"            => // do nothing
        case "Make another user an Admin" => println("Comming Soon!")
        case "Do Admin things"            => println("Comming Soon!")
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
    val list2 = List[String]("B", "C", "D", b)
    val list3 = List[String]("E", "F", "Unknown", "PEDAL", b)
    val list4 = List[String]("G", "H", b)

    //2016
    spark.sql("DROP TABLE IF EXISTS crash2016")
    spark.sql("CREATE TABLE IF NOT EXISTS crash2016(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
      "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
      "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/main/2016vp.csv' OVERWRITE INTO TABLE crash2016")
    //val df16 = spark.sql("SELECT * FROM crash2017")
    val df16 = spark.sql("SELECT sum(fatals) as fatalities, year from crash2016 group by year")
    //df16.show()

    //2017
    spark.sql("DROP TABLE IF EXISTS crash2017")
    spark.sql("CREATE TABLE IF NOT EXISTS crash2017(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
      "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
      "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/main/2017vp.csv' OVERWRITE INTO TABLE crash2017")
    //val df17 = spark.sql("SELECT * FROM crash2017")
    val df17 = spark.sql("SELECT sum(fatals) as fatalities, year from crash2017 group by year")
    //df17.show()

    //2018
    spark.sql("DROP TABLE IF EXISTS crash2018")
    spark.sql("CREATE TABLE IF NOT EXISTS crash2018(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
      "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
      "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/main/2018vp.csv' OVERWRITE INTO TABLE crash2018")
    //val df18 = spark.sql("SELECT * FROM crash2018")
    val df18 = spark.sql("SELECT sum(fatals) as fatalities, year from crash2018 group by year")
    //df18.show()

    //2019
    //val df = spark.read.option("header", true).csv("input/2019v2.csv")
    spark.sql("DROP TABLE IF EXISTS crash2019")
    spark.sql("CREATE TABLE IF NOT EXISTS crash2019(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
      "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
      "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/main/2019vp.csv' OVERWRITE INTO TABLE crash2019")
    //val df = spark.sql("SELECT * FROM crash2019")
    //val df = spark.sql("SELECT sum(fatals), state, year from crash2019 group by year, state")
    val df19 = spark.sql("SELECT sum(fatals) as fatalities, year from crash2019 group by year")
    //df19.show()
    //df.select("state").distinct.show(57)
    //df.select("fatals", "state", "year").show()
    //df.show()



    //ALL
    //ALL YEARS 2016-2019
    import spark.implicits._
    //val rdd = spark.sparkContext.textFile("input/main/2016v2.csv,input/main/2017v2.csv,input/main/2018v2.csv,input/main/2019v2.csv")
    //val rdd = spark.sparkContext.parallelize(data)
    //val df = spark.read.option("header", true).csv("input/main/*")
    //val df = rdd.toDF()
    //df.show()
    //df.where("STATE = 'Alabama'").show()

    import org.apache.spark.storage.StorageLevel
    //val rdd2 = rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    //or
    //val df2 = df.persist(StorageLevel.MEMORY_ONLY_SER)

    //spark.sql("drop table if exists crash2016")
    //spark.sql("drop table if exists crash2017")
    //spark.sql("drop table if exists crash2018")
    //spark.sql("drop table if exists crash2019")
    //spark.sql("drop table if exists test")


    //---------------------------------------------------------------------------------------------------------------
    spark.sql("DROP TABLE IF EXISTS crashData")
    spark.sql("CREATE TABLE IF NOT EXISTS crashData(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
      "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
      "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/main/*' OVERWRITE INTO TABLE crashData")
    val dfAll = spark.sql("SELECT * FROM crashData")
    //dfAll.show()

    //Graph the trend of fatalities in the entire USA
    //println("Trend of fatalities in the entire USA from 2016 to 2019:")
    val dfAllUS = spark.sql("SELECT sum(fatals) as fatalities, year from crashData group by year order by year")
    //dfAllUS.show()

    //Graph the trend of fatalities in individual states
    //println("Trend of fatalities in individual states from 2016 to 2019:")
    val dfState = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData group by state, year \n" +
    "order by state, year")
    //dfState.show()

    //Trend by year for all states
    //States with highest crashes every year
    //println("States with highest crash fatality numbers every year: ")
    val dfState2016 = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2016 \n" +
      "group by state, year order by fatalities DESC LIMIT 8")
    val dfState2017 = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2017\n" +
      "group by state, year order by fatalities DESC LIMIT 8")
    val dfState2018 = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2018 \n" +
      "group by state, year order by fatalities DESC LIMIT 8")
    val dfState2019 = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2019 \n" +
      "group by state, year order by fatalities DESC LIMIT 8")
    //dfState2016.show()
    //dfState2017.show()
    //dfState2018.show()
    //dfState2019.show()
    //States with lowest crashes every year
    //println("States with lowest crash fatality numbers every year: ")
    val state2016down = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2016 \n" +
      "group by state, year order by fatalities LIMIT 8")
    val state2017down = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2017\n" +
      "group by state, year order by fatalities LIMIT 8")
    val state2018down = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2018 \n" +
      "group by state, year order by fatalities LIMIT 8")
    val state2019down = spark.sql("SELECT sum(fatals) as fatalities, year, state from crashData where year = 2019 \n" +
      "group by state, year order by fatalities LIMIT 8")
    //state2016down.show()
    //state2017down.show()
    //state2018down.show()
    //state2019down.show()







    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op)
    var continue = true
    val List1 = List[String]("A", b)
    val List2 = List[String]("B", "C", "D", b)
    val List3 = List[String]("E", "F", b)
    val List4 = List[String]("G", "H", b)

    while (continue) {
      getOption(op) match {
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
