package Dataframes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._


object aggregrations {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading dt.txt data")

    val df = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/dt.txt")
    df.show()

    println("Single Column Aggregation")

    val gr_df = df.groupBy("category")
      .agg(sum("amount").cast(IntegerType).as("Total_amount")
        , count("amount").as("Count"))
      .orderBy("Total_amount")
    gr_df.show()

    println("Grouping multiple columns")

    val mulgr_df = df.groupBy("category","spendby")
      .agg(sum("amount").cast(IntegerType).as("Total_amount")
        , count("amount").as("Count"))
      .orderBy("category")
    mulgr_df.show()
  }
}
