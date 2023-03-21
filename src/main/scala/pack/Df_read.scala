package pack

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object Df_read {
  def main(args:Array[String]):Unit= {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    println("Hello Coder")

    val txt_df = spark.read.format("csv").load("file:///Users/venu7/Desktop/Zeyobron/Data/uber.csv")
    txt_df.show()

    /*txt_df.createOrReplaceTempView("txt_tab")
    val final_df = spark.sql("select * from txt_tab where _c1= 'Gymnastics'")
    final_df.show()

    val jdf = spark.read.format("json").load("file:///Users/venu7/Desktop/Zeyobron/Data/devices.json")
    jdf.show()
    jdf.createOrReplaceTempView("json_tab")
    val final_json = spark.sql("select * from json_tab where humidity>60")
    final_json.show() */








  }

}

