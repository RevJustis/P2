import org.apache.spark.sql.DataFrame

object Visualization {
  def viz(df: DataFrame, n: String, u: String): Unit = {
    df.write
      .format("csv")
      .option("header", true)
      .mode("overwrite")
      .save(s"hdfs://localhost:9000/user/$u/$n.csv")
  }
}
