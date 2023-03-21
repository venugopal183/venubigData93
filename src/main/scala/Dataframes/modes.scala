package Dataframes

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



object modes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val  df = spark.read.format("json").load("file:////Users/venu7/Desktop/Zeyobron/Data/devices.json")
    df.write.format("json").mode("ignore").save("file:///Users/venu7/Desktop/Zeyobron/Data/json1write/")
  }
}