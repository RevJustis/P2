import P2._
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel

object Query {
  def rural(): Unit = {
    val ru = spark.read
      .option("header", true)
      .csv("input/mainPF/*")
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
      .csv("input/mainPF/*")
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
      .csv("input/mainPF/*")
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
      .csv("input/mainPF/*")
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

  ///Start Jessica

  def pedtotal(): Unit = {
    println("Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) 2016-2019: ")
    println(mainPF.where("A_PED == 1").count())

    println("Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) by year: ")
    val pedTotalByY =
      mainPF.where("A_PED == 1")
    pedTotalByY
      .groupBy("YEAR")
      .agg(
        functions
          .count("*")
          .as("PedTotalYear")
      )
      .orderBy("YEAR")
      .show()
  }

  def fatalities(): Unit = {
    println("Total number of pedestrian vehicle-related FATALITIES 2016-2019: ")
    println(mainPF.where("A_PED_F == 1").count())

    println("Total number of pedestrian vehicle-related FATALITIES by year: ")
    val pedFbyY =
      mainPF.where(
        "A_PED_F == 1"
      )
    pedFbyY
      .groupBy("YEAR")
      .agg(
        functions
          .count("*")
          .as("PedFatalsYear")
      )
      .orderBy("YEAR")
      .show(60)
  }

  def states(): Unit = {
    println("Pedestrian INJURIES (fatal and nonfatal) by state: ")
    val state = mainPF.where("A_PED == 1")
    state
      .groupBy("STATENAME")
      .agg(
        functions
          .count("*")
          .as("Total")
      )
      .orderBy("Total")
      .show(60)

    println("Pedestrian FATALITIES by state  ")
    state
      .groupBy("STATENAME")
      .agg(
        functions
          .count("*")
          .as("Total")
      )
      .orderBy("Total")
      .show(60)
  }

  def sex(): Unit = {
    println("Pedestrian INJURIES (fatal and nonfatal) by sex 2016-2019: ")
    val sex =
      AgeSexPF
        .where(
          "SEX = 1 OR SEX = 2 OR SEX = 9"
        )
        .groupBy("SEX")
        .agg(
          functions
            .count("SEX")
            .as("Total")
        )
        .orderBy("Total")
        .show()
  }

  def age(): Unit = {
    println("Pedestrian INJURIES (fatal and nonfatal) by age: ")

    val t1 = AgeSexPF.where("AGE<=15")
    val t2 = AgeSexPF.where("AGE<=23 AND AGE>=16")
    val t3 = AgeSexPF.where("AGE<=29 AND AGE>=24")
    val t4 = AgeSexPF.where("AGE<=39 AND AGE>=30")
    val t5 = AgeSexPF.where("AGE<=49 AND AGE>=40")
    val t6 = AgeSexPF.where("AGE<=59 AND AGE>=50")
    val t7 = AgeSexPF.where("AGE<=60 AND AGE>=60")
    val t8 = AgeSexPF.where("AGE>=70")
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
          .as("16-23 years")
      )
      .show()
    t3
      .agg(
        functions
          .count("*")
          .as("24-29 years")
      )
      .show()

    t4
      .agg(
        functions
          .count("*")
          .as("30-39 years")
      )
      .show()

    t5
      .agg(
        functions
          .count("*")
          .as("40-49 years")
      )
      .show()

    t6
      .agg(
        functions
          .count("*")
          .as("50-59 years")
      )
      .show()

    t7
      .agg(
        functions
          .count("*")
          .as("60-69 years")
      )
      .show()

    t8
      .agg(
        functions
          .count("*")
          .as("70+ years")
      )
      .show()

    println("Pedestrian FATALITIES by age: ")
    val t9 = AgeSexPF.where("AGE<=15")
    val t10 = AgeSexPF.where("AGE<=23 AND AGE>=16")
    val t11 = AgeSexPF.where("AGE<=29 AND AGE>=24")
    val t12 = AgeSexPF.where("AGE<=39 AND AGE>=30")
    val t13 = AgeSexPF.where("AGE<=49 AND AGE>=40")
    val t14 = AgeSexPF.where("AGE<=59 AND AGE>=50")
    val t15 = AgeSexPF.where("AGE<=60 AND AGE>=60")
    val t16 = AgeSexPF.where("AGE>=70")
    t9
      .agg(
        functions
          .count("*")
          .as("0-15 years")
      )
      .show()
    t10
      .agg(
        functions
          .count("*")
          .as("16-23 years")
      )
      .show()
    t11
      .agg(
        functions
          .count("*")
          .as("24-29 years")
      )
      .show()

    t12
      .agg(
        functions
          .count("*")
          .as("30-39 years")
      )
      .show()

    t13
      .agg(
        functions
          .count("*")
          .as("40-49 years")
      )
      .show()

    t14
      .agg(
        functions
          .count("*")
          .as("50-59 years")
      )
      .show()

    t15
      .agg(
        functions
          .count("*")
          .as("60-69 years")
      )
      .show()

    t16
      .agg(
        functions
          .count("*")
          .as("70+ years")
      )
      .show()
  }

  }

  def jonathan(): Unit = {
    spark.sql("select * from personskilled").show()
    spark
      .sql(
        "select year, passengerCars, buses, total1 as TotalExcludingMotorcyclesAndPed, " +
          "motorcycles as Delta, (total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as DeltaPED," +
          " total from personsKilled where year between 2008 and 2018"
      )
      .show()
  }

 ///Begin Patrick

  def usfatals(): Unit = {
    //Graph the trend of fatalities in the entire USA
    println("Trend of fatalities in the entire USA from 2016 to 2019:")
    val dfAllUS = spark.sql(
      "SELECT sum(fatals) as fatalities, year from crashData group by year order by year"
    )
    dfAllUS.persist(StorageLevel.MEMORY_ONLY_SER)
    dfAllUS.show()
  }

def statefatals(): Unit = {
    //Graph the trend of fatalities in individual states
    println("Trend of fatalities in individual states from 2016 to 2019:")
    val dfState = spark.sql(
      "SELECT sum(fatals) as fatalities, year, state from crashData group by state, year \n" +
        "order by state, year"
    )
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



