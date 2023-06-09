package Dataframes

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



object Jdbc {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("Reading data from database")
    //val sqldf = spark .read .format("jdbc") .option("url","jdbc:mysql:*******************") .option("driver","com.mysql.jdbc.Driver") .option("dbtable","cashdata") .option("user","*******") .option("password","********") .load
    //sqldf.show()
    val sqldf = spark
      .read
      .format("jdbc")
      .option("url","*************************************")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","cashdata")
      .option("user","*******")
      .option("password","********")
      .load








  }

}
