import org.apache.spark.sql.DataFrame

object Visualization {
  def viz(df: DataFrame): Unit = {
    df.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save("hdfs://localhost:9000/user/justis/future.csv")

  }
}
