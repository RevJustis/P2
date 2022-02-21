import P2._

object Query {
  def q1(): Unit = {
    val ru = spark.read
      .option("header", true)
      .csv("input/main/*")
      .where("A_RU == 1")

    println("Rural Fatalities by State")
    ru.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Rural Fatalities by Year")
    ru.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)
  }
  def q2(): Unit = {
    val ur = spark.read
      .option("header", true)
      .csv("input/main/*")
      .toDF()
      .where("A_RU == 2")

    println("Urban Fatalities by State")
    ur.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Urban Fatalities by Year")
    ur.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

  }
  def q3(): Unit = {
    val sub = spark.read
      .option("header", true)
      .csv("input/main/*")
      .toDF()
      .where("A_RU == 3")

    println("Unknown location type Fatalities by State")
    sub.groupBy("STATENAME").agg(functions.sum("FATALS").as("SUM"))
    sum.orderBy(functions.col("SUM").desc).show(60)

    println("Unknown location type Fatalities by Year")
    sub.groupBy("YEAR").agg(functions.sum("FATALS").as("SUM"))
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
  def q5(): Unit = {}
  def q6(): Unit = {}
  def q7(): Unit = {}
  def q8(): Unit = {}
  def q9(): Unit = {}
  def q10(): Unit = {}
  def q11(): Unit = {}

}
