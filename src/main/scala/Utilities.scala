import scala.io.StdIn.readLine
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import java.io.{File, PrintWriter}
import scala.Console.println

object Utilities {
  def junk(spark: SparkSession): Unit = {
    spark.sql(
      "set hive.exec.dynamic.partition.mode=nonstrict"
    ) // TODO USE THIS FOR A MORE COMPACT DELETE

    //spark.sql("DROP TABLE IF EXISTS branchbevs")
    //spark.sql("DROP TABLE IF EXISTS cons_a")
    //spark.sql("DROP TABLE IF EXISTS cons_b")
    //spark.sql("DROP TABLE IF EXISTS cons_c")
    //spark.sql("DROP TABLE IF EXISTS cons_aXb")
    //spark.sql("DROP TABLE IF EXISTS constot1")
    //spark.sql("DROP TABLE IF EXISTS constot2")
    //spark.sql("DROP TABLE IF EXISTS constot3")
    //spark.sql("DROP TABLE IF EXISTS constot4")
    //spark.sql("DROP TABLE IF EXISTS constot5")
    //spark.sql("DROP TABLE IF EXISTS constot6")
    //spark.sql("DROP TABLE IF EXISTS constot7")
    //spark.sql("DROP TABLE IF EXISTS constot8")
    //spark.sql("DROP TABLE IF EXISTS constot9")
    //spark.sql("DROP TABLE IF EXISTS constotall")
    //spark.sql("DROP TABLE IF EXISTS cons_tot_all")

//    spark.sql("CREATE TABLE IF NOT EXISTS branch_a (bev STRING, branch STRING)" +
//        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS branch_b (bev STRING, branch STRING)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS branch_c (bev STRING, branch STRING)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_b (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_c (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS Partitioned_abc(bev STRING) PARTITIONED BY (branches STRING)")

//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE branch_a")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branch_b")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branch_c")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE cons_b")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE cons_c")
//    spark.sql("INSERT OVERWRITE TABLE Partitioned_abc PARTITION(branches) SELECT bev,branch FROM all_branch")

//    Intersection of cons_a and cons_b \\
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_aXb AS SELECT * FROM cons_a INTERSECT SELECT * FROM cons_b")
//    spark.sql("CREATE TABLE IF NOT EXISTS all_branch AS SELECT * FROM branch_a UNION SELECT * FROM branch_b UNION SELECT * FROM branch_c")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_abc AS SELECT * FROM cons_a UNION SELECT * FROM cons_b UNION SELECT * FROM cons_c")
//
//    for (x <- 1 to 9) {
//      spark.sql(s"CREATE TABLE IF NOT EXISTS b${x}bevs AS SELECT bev FROM all_branch WHERE branch = 'Branch$x'")
//    }
//    for (x <- 1 to 9) {
//      spark.sql(s"CREATE TABLE IF NOT EXISTS bevTot$x AS SELECT $x AS branch, COUNT(bev) AS bevTot FROM " +
//        s"b${x}bevs")
//    }
//    for (x <- 1 to 9) {
//      val c = spark.sql(s"SELECT $x AS branch, SUM(count) AS cons FROM " +
//      s"b${x}bevs INNER JOIN cons_abc AS c ON c.bev = b${x}bevs.bev").collect()
//      val pw = new PrintWriter(new File(s"input/dumb$x.txt" ))
//      pw.write((c(0)(0)).toString + ',' + c(0)(1) + '\n')
//      pw.close()
//    }
    // for (x <- 1 to 9) {
    //   spark.sql(s"CREATE TABLE IF NOT EXISTS consTot$x (branch INT, consTot INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //   spark.sql(s"LOAD DATA LOCAL INPATH 'input/dumb$x.txt' OVERWRITE INTO TABLE consTot$x")
    // }
//    var s = "CREATE TABLE IF NOT EXISTS bevTotAll AS SELECT * FROM bevTot1 "
//    for (x <- 2 to 9) {
//      s = s + s"UNION SELECT * FROM bevTot$x "
//    }
//    spark.sql(s)

    // var s2 = "CREATE TABLE IF NOT EXISTS constotall AS SELECT * FROM constot1 "
    // for (x <- 2 to 9) {
    //   s2 = s2 + s"UNION SELECT * FROM constot$x "
    // }
    // spark.sql(s2)

    //    bevs common between BranchA and ConscountA \\
//      spark.sql("SELECT branch_a.branch, cons_a.bev, cons_a.count FROM branch_a " +
//        "INNER JOIN cons_a ON cons_a.bev = branch_a.bev ORDER BY branch_a.branch, cons_a.bev, cons_a.count").show()
  }

  def end(): Unit = {
    println("Thank you for your time, hope you enjoyed those queries!")
  }

  def chooseN(n: Byte): Byte = {
    // val temp = readLine()
    var input: Char = readLine().charAt(0)
    var inByte: Byte = 0
    var goodIn: Boolean = false

    n match {
      case 1 =>
        print(
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
              print("Sorry, but you have to choose '1', or '2': ");
              input = readChar()
          }
        }
      case 3 =>
        while (!goodIn) {
          input match {
            case '1' => goodIn = true; inByte = 1.toByte
            case '2' => goodIn = true; inByte = 2.toByte
            case '3' => goodIn = true; inByte = 3.toByte
            case _ =>
              print("Sorry, but you have to choose '1', '2', or '3': ");
              input = readChar()
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
              print("Sorry, but you have to choose '1', '2', '3', or '4': ");
              input = readChar()
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
              print(
                "Sorry, but you have to choose '1', '2', '3', '4', or '5': "
              ); input = readChar()
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
              print(
                "Sorry, but you have to choose '1', '2', '3', '4', '5', or '6': "
              ); input = readChar()
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
              print(
                "Sorry, but you have to choose '1', '2', '3', '4', '5', '6', or '7': "
              ); input = readChar()
          }
        }
    }
    inByte
  }
}
