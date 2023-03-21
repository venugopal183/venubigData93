package Streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
object Weather {
  def main(args:String):Unit={
    val conf = new SparkConf().setAppName("Weather").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading an xml weather data to check xml schema")
    val df = spark.read.format("xml").option("rowtag","txndata").load("file:///Users/venu7/Desktop/Zeyobron/Data/file6.xml")
    df.show(false)
    df.printSchema()
  }
}
