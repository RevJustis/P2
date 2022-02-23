import P2._
import Utilities._
import Visualization._
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.storage.StorageLevel

object Query {
  def rural(): Unit = {
    val ru = mainPF.where("A_RU == 1")

    println("Rural Fatalities by State")
    val sum = ru.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
    viz(sum, "rural_s", "justis")

    println("Rural Fatalities by Year")
    val sum2 = ru.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
    viz(sum2, "rural_y", "justis")
  }

  def urban(): Unit = {
    val ur = mainPF.where("A_RU == 2")

    println("Urban Fatalities by State")
    val sum = ur.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
    viz(sum, "urban_s", "justis")

    println("Urban Fatalities by Year")
    val sum2 = ur.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
    viz(sum, "urban_y", "justis")

  }

  def other(): Unit = {
    val other = mainPF.where("A_RU == 3")

    println("Unknown location type Fatalities by State")
    val sum = other.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Unknown location type Fatalities by Year")
    val sum2 = other.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

  }

  ///Start Jessica/Topic 1

  def pedtotal(): Unit = {
    println(
      "Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) 2016-2019: "
    )
    println(mainPF.where("A_PED == 1").count())

    println(
      "Total number of pedestrian vehicle-related INJURIES (fatal and non-fatal) by year: "
    )
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
      ageSexPF
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

    val t1 = ageSexPF.where("AGE<=15")
    val t2 = ageSexPF.where("AGE<=23 AND AGE>=16")
    val t3 = ageSexPF.where("AGE<=29 AND AGE>=24")
    val t4 = ageSexPF.where("AGE<=39 AND AGE>=30")
    val t5 = ageSexPF.where("AGE<=49 AND AGE>=40")
    val t6 = ageSexPF.where("AGE<=59 AND AGE>=50")
    val t7 = ageSexPF.where("AGE<=60 AND AGE>=60")
    val t8 = ageSexPF.where("AGE>=70")
    val x: List[(DataFrame, String)] =
      List(
        (t1, "0-15 years"),
        (t2, "16-23"),
        (t3, "24-29"),
        (t4, "30-39"),
        (t5, "40-49"),
        (t6, "50-59"),
        (t7, "60-69"),
        (t8, "70+")
      )
    x.foreach(tup => tup._1.agg(functions.count("*").as(tup._2)).show())

    println("Pedestrian FATALITIES by age: ")
    val t9 = ageSexPF.where("AGE<=15 AND A_PED_F=1")
    val t10 = ageSexPF.where("AGE<=23 AND AGE>=16 AND A_PED_F=1")
    val t11 = ageSexPF.where("AGE<=29 AND AGE>=24 AND A_PED_F=1")
    val t12 = ageSexPF.where("AGE<=39 AND AGE>=30 AND A_PED_F=1")
    val t13 = ageSexPF.where("AGE<=49 AND AGE>=40 AND A_PED_F=1")
    val t14 = ageSexPF.where("AGE<=59 AND AGE>=50 AND A_PED_F=1")
    val t15 = ageSexPF.where("AGE<=60 AND AGE>=60 AND A_PED_F=1")
    val t16 = ageSexPF.where("AGE>=70 AND A_PED_F=1")
    val l: List[(DataFrame, String)] =
      List(
        (t9, "0-15 years AND A_PED_F=1"),
        (t10, "16-23 AND A_PED_F=1"),
        (t11, "24-29 AND A_PED_F=1"),
        (t12, "30-39 AND A_PED_F=1"),
        (t13, "40-49 AND A_PED_F=1"),
        (t14, "50-59 AND A_PED_F=1"),
        (t15, "60-69 AND A_PED_F=1"),
        (t16, "70+ AND A_PED_F=1")
      )
    l.foreach(tup => tup._1.agg(functions.count("*").as(tup._2)).show())
  }

  //Start Jonathan's
  def jonathan(): Unit = {
    spark
      .sql(
        "select year, passengerCars, buses, total1 as TotalExcludingMotorcyclesAndPed, " +
          "motorcycles as Delta, (total1 + motorcycles) as TotalExcludingPed, abs((total1 + motorcycles) - total) as DeltaPED," +
          " total from personsKilled where year between 2008 and 2018"
      )
      .show()
  }

  def pedal(): Unit = {
    val pedal = mainPF.where("A_PEDAL_F == 1")

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

  //Start Patrick's
  def usfatals(): Unit = {
    //Graph the trend of fatalities in the entire USA
    println("Trend of fatalities in the entire USA from 2016 to 2019:")
    val dfAllUS = spark.sql(
      "SELECT sum(fatals) as fatalities, year from crashData group by year order by year"
    )
    dfAllUS.persist(StorageLevel.MEMORY_ONLY_SER)
    dfAllUS.show()
    viz(dfAllUS, "usfatals", "patrickbrown")
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
    viz(dfState, "statefatals", "patrickbrown")
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
    viz(x, "vehicleCrash", "patrickbrown")
  }
}
