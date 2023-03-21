package Dataframes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object localsql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val loc_sql = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/itv003173")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","Sales_Table_itv0003173")
      .option("user","root")
      .option("password","Prok7@tgos17")
      .load()
    loc_sql.show()
  }

}
