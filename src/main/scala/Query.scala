import P2._
import org.apache.spark.sql.functions

object Query {
  def q1(): Unit = {
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
  def q2(): Unit = {
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
  def q3(): Unit = {
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
  def q4(): Unit = {
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

  def q5(): Unit = {
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
  def q6(): Unit = {}
  def q7(): Unit = {}
  def q8(): Unit = {}
  def q9(): Unit = {}
  def q10(): Unit = {}
  def q11(): Unit = {}

}