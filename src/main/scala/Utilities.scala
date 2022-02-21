import scala.io.StdIn.readLine
import org.apache.spark.sql.{SparkSession, functions}
import java.io.{File, FileOutputStream, PrintWriter}
import P2._
import org.apache.spark.storage.StorageLevel

object Utilities {
  var admin: Boolean = false
  def prep(): Unit = {
    spark.sql(
      "set hive.exec.dynamic.partition.mode=nonstrict"
    )
    spark.sql("DROP TABLE IF EXISTS userpass")
    spark.sql(
      "CREATE TABLE IF NOT EXISTS userpass (user STRING, pass STRING, admin STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/userpass.txt' OVERWRITE INTO TABLE userpass"
    )
    //Jonathan
    spark.sql(
      "CREATE TABLE IF NOT EXISTS personsKilled (year int, passengerCars int, lightTrucks int, largeTrucks int," +
        "motorcycles int, buses int, otherUnknown int, total1 int, pedestrian int, pedalcyclist int, other int, total2 int," +
        "unknownPersonType int, total int)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    )
    spark.sql(
      "LOAD DATA LOCAL INPATH 'input/PersonsKilled/PersonsKilled.csv' OVERWRITE INTO TABLE personsKilled"
    )
  }

  def end(): Unit = {
    println("Thank you for your time, hope you enjoyed those queries!")
  }

  def chooseN(n: Byte): Byte = {
    var input: Char = readLine().trim().charAt(0)
    var inByte: Byte = 0
    var goodIn: Boolean = false

    n match {
      case 1 =>
        println(
          "Sorry, but you have to choose '1'... Huh... almost feels like you have no choice at all... OK you can go now. "
        );
        goodIn = true;
        inByte = 1.toByte
      case 2 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case _ =>
              println("Sorry, but you have to choose '1', or '2': ");
              input = readLine().trim().charAt(0)
          }
        }
      case 3 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case _ =>
              println("Sorry, but you have to choose '1', '2', or '3': ");
              input = readLine().trim().charAt(0)
          }
        }
      case 4 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case _ =>
              println("Sorry, but you have to choose '1', '2', '3', or '4': ");
              input = readLine().trim().charAt(0)
          }
        }
      case 5 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case '5' => goodIn = true; inByte = 5.toByte
            case _ =>
              println(
                "Sorry, but you have to choose '1', '2', '3', '4', or '5': "
              )
              input = readLine().trim().charAt(0)
          }
        }
      case 6 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case '5' => goodIn = true; inByte = 5.toByte
            case '6' => goodIn = true; inByte = 6.toByte
            case _ =>
              println(
                "Sorry, but you have to choose '1', '2', '3', '4', '5', or '6': "
              )
              input = readLine().trim().charAt(0)
          }
        }
      case 7 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case '4' => goodIn = true; inByte = 4.toByte
            case '5' => goodIn = true; inByte = 5.toByte
            case '6' => goodIn = true; inByte = 6.toByte
            case '7' => goodIn = true; inByte = 7.toByte
            case _ =>
              println(
                "Sorry, but you have to choose '1', '2', '3', '4', '5', '6', or '7': "
              )
              input = readLine().trim().charAt(0)
          }
        }
    }
    inByte
  }

  def qMenu(options: List[String]): Unit = {
    var continue = false
    while (!continue) {
      getOption(options) match {
        case "Rural" => // rural
          val ru = spark.read
            .option("header", true)
            .csv("input/main/*")
            .where("A_RU == 1")
          /* ru.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/rural.csv")
           */
          println("Rural Fatalities by State")
          val sum =
            ru.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
          sum.orderBy(functions.col("SUM").desc).show(60)
        /*
        sum.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/rural.csv")
         */
        case "Urban" => // urban
          val ur = spark.read
            .option("header", true)
            .csv("input/main/*")
            .toDF()
            .where("A_RU == 2")
          /*
        ur.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/urban.csv")
           */
          println("Urban Fatalities by State")
          val sum =
            ur.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
          sum.orderBy(functions.col("SUM").desc).show(60)
        /*
        sum.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/rural.csv")
         */
        case "Unknown" => // suburban
          val sub = spark.read
            .option("header", true)
            .csv("input/main/*")
            .toDF()
            .where("A_RU == 3")
          /* hdfs for zeppelin
        ur.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/urban.csv")
           */
          println("Suburban Fatalities by State")
          val sum =
            sub.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
          sum.orderBy(functions.col("SUM").desc).show(60)
        /*
        sum.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/rural.csv")
         */
        case "PEDAL" =>
          val pedal = spark.read
            .option("header", true)
            .csv("input/main/*")
            .toDF()
            .where("A_PEDAL_F == 1")
          /* hdfs for zeppelin
        ur.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/pedal.csv")
           */
          println("Number of crashes fatal to Cyclists by state")
          pedal
            .groupBy("STATENAME")
            .count()
            .orderBy(functions.col("count").desc)
            .show(56)
        /*
        sum.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/justis/pedal_state.csv")
         */
        case "US Fatal" => //GRAPH TRENDS OF FATALITIES IN ENTIRE U.S. FOR 4 YEARS:
          //CREATE TABLE OF ALL DATA
          //val peopleDF = spark.read.option("input/vehicleStats/*")
          val aDF = spark.read.option("header", true).csv("input/main_p/*")
          // DataFrames can be saved as Parquet files, maintaining the schema information
          aDF.write
            .mode("overwrite")
            .parquet("spark-warehouse/usCrashes.parquet")
          // Read in the parquet file created above
          // Parquet files are self-describing so the schema is preserved
          // The result of loading a Parquet file is also a DataFrame
          val parquetFileDF =
            spark.read.parquet("spark-warehouse/usCrashes.parquet")
          // Parquet files can also be used to create a temporary view and then used in SQL statements
          parquetFileDF.createOrReplaceTempView("crashData")
          val dfAll = spark.sql("select * from crashData")
          //dfAll.show()
          //Optimization
          dfAll.persist(StorageLevel.MEMORY_ONLY_SER)

          //Graph the trend of fatalities in the entire USA
          println("Trend of fatalities in the entire USA from 2016 to 2019:")
          val dfAllUS = spark.sql(
            "SELECT sum(fatals) as fatalities, year from crashData group by year order by year"
          )
          dfAllUS.show()
          //Optimization
          dfAllUS.persist(StorageLevel.MEMORY_ONLY_SER)
        /*
          df.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/patrickbrown/future.csv")
         */

        case "State Fatal" => //GRAPH TRENDS OF FATALITIES IN EACH STATE:
          //CREATE TABLE OF ALL DATA
          //val peopleDF = spark.read.option("input/vehicleStats/*")
          val aDF = spark.read.option("header", true).csv("input/main_p/*")
          // DataFrames can be saved as Parquet files, maintaining the schema information
          aDF.write
            .mode("overwrite")
            .parquet("spark-warehouse/usCrashes.parquet")
          // Read in the parquet file created above
          // Parquet files are self-describing so the schema is preserved
          // The result of loading a Parquet file is also a DataFrame
          val parquetFileDF =
            spark.read.parquet("spark-warehouse/usCrashes.parquet")
          // Parquet files can also be used to create a temporary view and then used in SQL statements
          parquetFileDF.createOrReplaceTempView("crashData")
          val dfAll = spark.sql("select * from crashData")
          //dfAll.show()
          //Optimization
          dfAll.persist(StorageLevel.MEMORY_ONLY_SER)

          //Graph the trend of fatalities in individual states
          println("Trend of fatalities in individual states from 2016 to 2019:")
          val dfState = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData group by state, year \n" +
              "order by state, year"
          )
          dfState.show()
          //Optimization
          dfState.persist(StorageLevel.MEMORY_ONLY_SER)
        /*
        ur.write
          .format("csv")
          .option("header", true)
          .mode("overwrite")
          .save("hdfs://localhost:9000/user/patrickbrown/future.csv")
         */

        case "Safest" => //WHICH STATES ARE THE SAFEST?
          //CREATE TABLE OF ALL DATA
          //val peopleDF = spark.read.option("input/vehicleStats/*")
          val aDF = spark.read.option("header", true).csv("input/main_p/*")
          // DataFrames can be saved as Parquet files, maintaining the schema information
          aDF.write
            .mode("overwrite")
            .parquet("spark-warehouse/usCrashes.parquet")
          // Read in the parquet file created above
          // Parquet files are self-describing so the schema is preserved
          // The result of loading a Parquet file is also a DataFrame
          val parquetFileDF =
            spark.read.parquet("spark-warehouse/usCrashes.parquet")
          // Parquet files can also be used to create a temporary view and then used in SQL statements
          parquetFileDF.createOrReplaceTempView("crashData")
          val dfAll = spark.sql("select * from crashData")
          //Optimization
          dfAll.persist(StorageLevel.MEMORY_ONLY_SER)

          //Trend by year for all states
          //States with highest crashes every year
          println("States with highest crash fatality numbers every year: ")
          val dfState2016 = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2016 \n" +
              "group by state, year order by fatalities DESC LIMIT 8"
          )
          val dfState2017 = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2017\n" +
              "group by state, year order by fatalities DESC LIMIT 8"
          )
          val dfState2018 = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2018 \n" +
              "group by state, year order by fatalities DESC LIMIT 8"
          )
          val dfState2019 = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2019 \n" +
              "group by state, year order by fatalities DESC LIMIT 8"
          )
          dfState2016.show()
          dfState2017.show()
          dfState2018.show()
          dfState2019.show()
          //Optimization
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          //States with lowest crashes every year
          println("States with lowest crash fatality numbers every year: ")
          val state2016down = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2016 \n" +
              "group by state, year order by fatalities LIMIT 8"
          )
          val state2017down = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2017\n" +
              "group by state, year order by fatalities LIMIT 8"
          )
          val state2018down = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2018 \n" +
              "group by state, year order by fatalities LIMIT 8"
          )
          val state2019down = spark.sql(
            "SELECT sum(fatals) as fatalities, year, state from crashData where year = 2019 \n" +
              "group by state, year order by fatalities LIMIT 8"
          )
          state2016down.show()
          state2017down.show()
          state2018down.show()
          state2019down.show()
          //Optimization
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
          dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)

          //See what type of vehicle led to the most crashes.
          //val peopleDF = spark.read.option("input/vehicleStats/*")
          val vDF =
            spark.read.option("header", true).csv("input/vehicleStats/*").toDF()
          // DataFrames can be saved as Parquet files, maintaining the schema information
          vDF.write.mode("overwrite").parquet("spark-warehouse/vehicle.parquet")
          // Read in the parquet file created above
          // Parquet files are self-describing so the schema is preserved
          // The result of loading a Parquet file is also a DataFrame
          val parquetDF = spark.read.parquet("spark-warehouse/vehicle.parquet")
          // Parquet files can also be used to create a temporary view and then used in SQL statements
          parquetDF.createOrReplaceTempView("vehicleParquetFile")
          println("Here are the stats for different vehicles: ")
          val x = spark.sql(
            "select * from vehicleParquetFile order by Year, VehicleType"
          )
          x.show(28)
          //Optimization
          x.persist(StorageLevel.MEMORY_ONLY_SER)
        case "q4.1" =>
          spark
            .sql(
              "select year, passengerCars, buses, total1 as TotalExcludingMotorcyclesAndPed, " +
                "motorcycles as Delta, (total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as DeltaPED," +
                " total from personsKilled where year between 2008 and 2018"
            )
            .show()
        case "jessica1" =>
          spark.sql(
            "CREATE TABLE IF NOT EXISTS test (year STRING, total STRING)" +
              "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
          )
          //  spark.sql(
          // "LOAD DATA LOCAL INPATH 'input/FileName.txt' OVERWRITE INTO TABLE test"
          // )
          // spark.sql("select * from test").show

          val main = spark.read
            .option("header", true)
            .csv("input/Main/*")

          val AgeSex = spark.read
            .option("header", true)
            .csv("input/AgeSex/*")

          println(
            "Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) 2016-2019: "
          )
          println(
            main.where("A_PED == 1").count()
          ) //result is same if you use both columns
          println(
            AgeSex.where("A_PED == 1").count()
          ) //this code using the other dataset AgeSex returns
          //a slightly higher number

          println(
            "Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) by year: "
          )
          val pedTotalByY =
            main.where("A_PED == 1") // Justis was here
          pedTotalByY
            .groupBy("YEAR")
            .agg(
              functions
                .count("*")
                .as("PedTotalYear")
            )
            .orderBy("YEAR")
            .show()

          println(
            "Total number of pedestrian vehicle-related FATALITIES 2016-2019: "
          )
          println(main.where("A_PED_F == 1").count())

          //println(AgeSex.where("A_PED_F == 1").count())//used the other dataset here and got a different result
          // from above, both numbers seem really high

          println(
            "Total number of pedestrian vehicle-related FATALITIES by year: "
          ) //number result seems really high
          val pedFbyY =
            main.where(
              "A_PED_F == 1"
            ) //why do I get ridiculous number when == to "2"??
          pedFbyY
            .groupBy("YEAR")
            .agg(
              functions
                .count("*")
                .as("PedFatalsYear")
            )
            .orderBy("YEAR")
            .show(60)

          println(
            "Pedestrian INJURIES (fatal and nonfatal) by state: "
          ) //could do min/max here as well
          val state = main.where("A_PED == 1")

          state
            .groupBy("STATENAME")
            .agg(
              functions
                .count("*")
                .as("Total")
            )
            .orderBy("Total")
            .show(60)

          println(
            "Pedestrian FATALITIES by state  "
          ) //could do min/max here as well
          main
            .where("A_PED_F == 1")
            .groupBy("STATENAME")
            .agg(
              functions
                .count("*")
                .as("Total")
            )
            .orderBy("Total")
            .show(60)

          println("Pedestrian INJURIES (fatal and nonfatal) by sex 2016-2019: ")
          val sex =
            AgeSex
              .where(
                "SEX = 1 OR SEX = 2 OR SEX = 9"
              ) // 9 is apparently "other" or "unknown" here; archaic
              .groupBy("SEX")
              .agg(
                functions
                  .count("SEX")
                  .as("Total")
              )
              .orderBy("Total")
              .show()

          //the code below does return total number of injuries for this age range, but I would have to do a separate
          //function for each range if I use this method. I think a partition would work, but I'm still working on how
          //to do that
          println("Pedestrian INJURIES (fatal and nonfatal) by age: ")
          val t1 = AgeSex.where("AGE<=15")
          val t2 = AgeSex.where("AGE<=15")
          val t3 = AgeSex.where("AGE<=15")
          t1
            .agg(
              functions
                .count("*")
                .as("0-15 years")
            )
            .show()
          t2
            .agg(
              functions
                .count("*")
                .as("??? years")
            )
            .show()
          t3
            .agg(
              functions
                .count("*")
                .as("??? years")
            )
            .show()

        // val t1 = (AgeSex.where("AGE<=15").toDF(), "0-15 years")
        // val t2 = (AgeSex.where("AGE<=15").toDF(), "2")
        // val t3 = (AgeSex.where("AGE<=15").toDF(), "3")
        // val a = new Array[(DataFrame, String)](t1, t2, t3)

        // for (e <- a) {
        //   e._1
        //     .agg(
        //       functions
        //         .count("*")
        //         .as(e._2)
        //     )
        //     .show()
        // }
        case b => continue = true
      }
    }
  }

  def signUp(): String = {
    var continue = false
    var user = ""
    while (!continue) {
      println("Please enter a Username:")
      user = readLine().trim()
      if (userExists(user)) println("That username is taken, try again:")
      else continue = true
    }
    println("Please enter a Password:")
    val pass = readLine()

    val pw = new PrintWriter(
      new FileOutputStream(
        new File("input/userpass.txt"),
        true // append = true
      )
    )
    pw.append(s"$user,$pass,false\n")
    pw.close()
    prep()
    user
  }

  def logIn(user: String): Unit = {
    val q = spark.sql(
      s"SELECT admin FROM userpass WHERE user = '$user' AND admin = 'true'"
    )
    if (q.count() == 0) admin = false else admin = true
  }

  def userExists(user: String): Boolean = {
    val q = spark.sql(s"SELECT * FROM userpass WHERE user = '$user'")
    if (q.count() == 0) false else true
  }

  def authPass(user: String, pass: String): Boolean = {
    val q = spark.sql(
      s"SELECT * FROM userpass WHERE user = '$user' AND pass = '$pass'"
    )
    if (q.count() == 0) false else true
  }

  def getOption(l: List[String]): String = {
    val menu = new MyMenu(l)
    menu.printMenu()
    val in = chooseN(l.length.toByte)
    menu.selectOption(in)
  }

  def junk(): Unit = {
    // JUNK FROM JONATHAN
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
    //---------------------------------------------------------------------------------------------------------------
    //PATRICK'S JUNK:
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
    /*
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
     */

    //ALL
    //ALL YEARS 2016-2019
    //import spark.implicits._
    //val rdd = spark.sparkContext.textFile("input/main/2016v2.csv,input/main/2017v2.csv,input/main/2018v2.csv,input/main/2019v2.csv")
    //val rdd = spark.sparkContext.parallelize(data)
    //val df = spark.read.option("header", true).csv("input/main/*")
    //val df = rdd.toDF()
    //df.show()
    //df.where("STATE = 'Alabama'").show()

    //spark.sql("DROP TABLE IF EXISTS crashData")
    //spark.sql("CREATE TABLE IF NOT EXISTS crashData(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
    //  "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
    //  "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
    //  "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/main/*' OVERWRITE INTO TABLE crashData")
    //val dfAll = spark.sql("SELECT * FROM crashData")
    //dfAll.show()

    //CREATE TABLE OF ALL DATA
    //spark.sql("DROP TABLE IF EXISTS crashData")
    //spark.sql("CREATE TABLE IF NOT EXISTS crashData(crashType int, age15_19 int, age15_20 int, age16_19 int, age16_20 int, \n" +
    //  "age16_24 int, age21_24 int, age60plus int, motorcyle int, pedestrian int, pedalcyclist int, pedalFatal int, pedestrianFatal int, \n" +
    //  "relationToRoad int, ruralUrban int, fatals int, schoolBus int, stateNum int, state String, stateCase int, year int) \n" +
    //  "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/main/*' OVERWRITE INTO TABLE crashData")
    //val dfAll = spark.sql("SELECT * FROM crashData")
    //dfAll.show()

    //NEW WAY
    //spark.sql("drop table if exists vehicleStats")
    //val df = spark.read.option("header", true).csv("input/vehicleStats/*")
    //df.show(50)
    //OLD WAY
    //spark.sql("drop table if exists vehicleStats")
    //spark.sql("create table if not exists vehicleStats(vehicleType varchar(30), total int, percent double, \n" +
    //"year int) row format delimited fields terminated by ','")
    //spark.sql("load data local inpath 'input/vehicleStats/*' overwrite into table vehicleStats")

    //Read CSV file into a DF
    //val vDF = spark.read.option("header", true).csv("input/vehicleStats/*")
    //vDF.write.parquet("spark-warehouse/vehiclestats/vehicleStats.parquet")
    //vDF.createOrReplaceTempView("vehicleStats")
    //val dfVehicle = spark.sql("select * from vehicleStats")
    //dfVehicle.show()

    //vDF.write.parquet("vehicleStats.parquet")
    //vDF.createOrReplaceTempView("vehicleStats")
    //val dfVehicle = spark.sql("select * from vehicleStats order by vehicleType, year")
    //dfVehicle.show(30)

    //import org.apache.spark.storage.StorageLevel
    //val rdd2 = rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    //or
    //val df2 = df.persist(StorageLevel.MEMORY_ONLY_SER)

    //spark.sql("drop table if exists crash2016")
    //spark.sql("drop table if exists crash2017")
    //spark.sql("drop table if exists crash2018")
    //spark.sql("drop table if exists crash2019")
    //spark.sql("drop table if exists test")

    /*
   <component name="SbtModule">
     <option name="buildForURI" value="file:$MODULE_DIR$/../../" />
     <option name="imports" value="SUB:DOLLAR05e87506ba00c849e00d.`root`, _root_.sbt.Keys._, _root_.sbt.ScriptedPlugin.autoImport._, _root_.sbt.plugins.JUnitXmlReportPlugin.autoImport._, _root_.sbt.plugins.MiniDependencyTreePlugin.autoImport._, _root_.sbt._, _root_.sbt.nio.Keys._, _root_.sbt.plugins.IvyPlugin, _root_.sbt.plugins.JvmPlugin, _root_.sbt.plugins.CorePlugin, _root_.sbt.ScriptedPlugin, _root_.sbt.plugins.SbtPlugin, _root_.sbt.plugins.SemanticdbPlugin, _root_.sbt.plugins.JUnitXmlReportPlugin, _root_.sbt.plugins.Giter8TemplatePlugin, _root_.sbt.plugins.MiniDependencyTreePlugin, _root_.scala.xml.{TopScope=&gt;SUB:DOLLARscope}" />
   </component>

     */

    //END OF PATRICK'S JUNK.
  }
}
