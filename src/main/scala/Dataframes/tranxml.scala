package Dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}


object tranxml {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val xml_df = spark.read.format("xml").option("rowtag","POSLog").load("file:///Users/venu7/Desktop/Zeyobron/Data/transactions.xml")
    xml_df.show()

    xml_df.printSchema()


  }

}
