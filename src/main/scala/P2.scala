import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
    /*
    spark.sql(
      "CREATE TABLE IF NOT EXISTS test (year STRING, total STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/FileName.txt' OVERWRITE INTO TABLE test"
    )
    spark.sql("select * from test").show
     */

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












    /*
    println("the last one?")
    df.where("STATE == 57").show

    println("where is samoa?") // here it is!
    df.where("STATE == 3").select(sum("FATALS")).show

    println("not rural or urban?")
    df.where("A_RU == 3").show
    df.where("A_RU == 0").show

     */
    /*
    df.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/patrickbrown/future.csv")
     */

    /*
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

     */
    spark.close
    end
  }
}
