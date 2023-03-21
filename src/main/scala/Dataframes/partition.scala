package Dataframes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object partition {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark= SparkSession.builder.getOrCreate()
    import spark.implicits._

    println("Reading uscountry.csv from local")

    val raw_data = spark.read.format("csv").option("header","true").load(("file:///Users/venu7/Desktop/Zeyobron/Data/usdata.csv"))
    raw_data.show()

    println("Partitioning the data by using State as a partition column")

    raw_data.write.format("csv").mode("overwrite").option("header","true").partitionBy("state").save("file:///Users/venu7/Desktop/Zeyobron/Data/Partitioned_Data/")
    println("SUCCESS!!")








  }



}
