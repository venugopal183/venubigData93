package Dataframes

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object xmlread{


  def main(args:Array[String]):Unit={


    val conf = new SparkConf().setMaster("local[*]").setAppName("first")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .format("xml")
      .option("rowtag","book")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/book.xml")

    df.show()


  }
}
