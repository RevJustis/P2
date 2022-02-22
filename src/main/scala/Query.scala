import P2._
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel

object Query {
  def rural(): Unit = {
    val ru = spark.read
      .option("header", true)
      .csv("input/main/*")
      .where("A_RU == 1")

    println("Rural Fatalities by State")
    val sum = ru.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Rural Fatalities by Year")
    val sum2 = ru.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
  }
  def urban(): Unit = {
    val ur = spark.read
      .option("header", true)
      .csv("input/main/*")
      .toDF()
      .where("A_RU == 2")

    println("Urban Fatalities by State")
    val sum = ur.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Urban Fatalities by Year")
    val sum2 = ur.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

  }
  def other(): Unit = {
    val other = spark.read
      .option("header", true)
      .csv("input/main/*")
      .toDF()
      .where("A_RU == 3")

    println("Unknown location type Fatalities by State")
    val sum = other.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Unknown location type Fatalities by Year")
    val sum2 = other.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

  }
  def pedal(): Unit = {
    val pedal = spark.read
      .option("header", true)
      .csv("input/main/*")
      .toDF()
      .where("A_PEDAL_F == 1")

    println("Number of crashes fatal to Cyclists by state")
    pedal
      .groupBy("STATENAME")
      .count()
      .orderBy(functions.col("count").desc)
      .show(56)

    println("Number of crashes fatal to Cyclists by Year")
    pedal
      .groupBy("YEAR")
      .count()
      .orderBy(functions.col("count").desc)
      .show(56)
  }

  def jessica(): Unit = {
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

    println("Pedestrian FATALITIES by state  ") //could do min/max here as well
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

  }

  def jonathan(): Unit = {
    spark.sql("select * from personskilled").show()
//    spark.sql("select * from personskilled").show()

    spark
      .sql(
        "select year, passengerCars, buses, total1 as TotalExcludingMotorcyclesAndPed, " +
          "motorcycles as Delta, (total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as DeltaPED," +
          " total from personsKilled where year between 2008 and 2018"
      )
      .show()

    // "(total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as Delta, total from " +
    // "personsKilled where year between 2008 and 2018").show()

  }

  def usfatals(): Unit = {
    //CREATE TABLE OF ALL DATA
    //val peopleDF = spark.read.option("input/vehicleStats/*")
    val aDF = spark.read.option("header", true).csv("input/main_p/*")
    //Optimization
    aDF.persist(StorageLevel.MEMORY_ONLY_SER)
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
      .save("hdfs://localhost:9000/user/patrickbrown/usfatals.csv")
     */
  }
  def statefatals(): Unit = {
    //CREATE TABLE OF ALL DATA
    //val peopleDF = spark.read.option("input/vehicleStats/*")
    val aDF = spark.read.option("header", true).csv("input/main_p/*")
    //Optimization
    aDF.persist(StorageLevel.MEMORY_ONLY_SER)
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

    //Graph the trend of fatalities in individual states
    println("Trend of fatalities in individual states from 2016 to 2019:")
    val dfState = spark.sql(
      "SELECT sum(fatals) as fatalities, year, state from crashData group by state, year \n" +
        "order by state, year"
    )
    //Optimization
    dfState.persist(StorageLevel.MEMORY_ONLY_SER)
    dfState.show()
    /*
    ur.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/patrickbrown/state_fatals.csv")
     */
  }
  def highfatalstates(): Unit = {
    //CREATE TABLE OF ALL DATA
    //val peopleDF = spark.read.option("input/vehicleStats/*")
    val aDF = spark.read.option("header", true).csv("input/main_p/*")
    //Optimization
    aDF.persist(StorageLevel.MEMORY_ONLY_SER)
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
    //Optimization
    dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
    dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
    dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
    dfState2016.persist(StorageLevel.MEMORY_ONLY_SER)
    dfState2016.show()
    dfState2017.show()
    dfState2018.show()
    dfState2019.show()
    /*
    ur.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/patrickbrown/highfatalstates.csv")
     */
  }
  def lowfatalstates(): Unit = {
    //CREATE TABLE OF ALL DATA
    //val peopleDF = spark.read.option("input/vehicleStats/*")
    val aDF = spark.read.option("header", true).csv("input/main_p/*")
    //Optimization
    aDF.persist(StorageLevel.MEMORY_ONLY_SER)
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
    //Optimization
    state2016down.persist(StorageLevel.MEMORY_ONLY_SER)
    state2017down.persist(StorageLevel.MEMORY_ONLY_SER)
    state2018down.persist(StorageLevel.MEMORY_ONLY_SER)
    state2019down.persist(StorageLevel.MEMORY_ONLY_SER)
    state2016down.show()
    state2017down.show()
    state2018down.show()
    state2019down.show()
    /*
    ur.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/patrickbrown/lowfatalstates.csv")
     */
  }
  def vehicleCrash(): Unit = {
    //See what type of vehicle led to the most crashes.
    val vDF =
      spark.read.option("header", true).csv("input/vehicleStats/*").toDF()
    //Optimization
    vDF.persist(StorageLevel.MEMORY_ONLY_SER)
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
    //Optimization
    x.persist(StorageLevel.MEMORY_ONLY_SER)
    x.show(28)
    /*
    ur.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/patrickbrown/vehicle_crash_data.csv")
     */
  }
  def q9(): Unit = {}
  def q10(): Unit = {}
  def q11(): Unit = {}

}
