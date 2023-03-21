package Dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object complex_data {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("====reading multiline data====")

    val raw_data = spark.read
      .format("json")
      .option("multiline", "true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/jc.json")
    //raw_data.show()
    raw_data.printSchema()

    println("====Flattening the Complex Data====")

    val flat_data = raw_data.
      select("Years",
      "address.permanentAddress",
        "address.temporaryAddress",
        "org",
        "trainer")
    flat_data.show()
    flat_data.printSchema()

    println("===Flattening Data using WithColumn====")

    val with_flat_data = raw_data.withColumn("permanentAddress",expr("address.permanentAddress"))
      .withColumn("temporaryAddress",expr("address.temporaryAddress"))
      .drop("address")
    with_flat_data.show()
    with_flat_data.printSchema()

    println("===Reading pic.json===")

    val raw_pic = spark.read
      . format("json")
      .option("multiline","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/pic.json")
    raw_pic.show()
    raw_pic.printSchema()

    println("===Flattening three columns in pic.json===")
    val pic_flat = raw_pic
      .withColumn("height", expr("image.height"))
      .withColumn("url",expr("image.url"))
      .withColumn("width",expr("image.width"))
      .drop("image")

    pic_flat.show()
    pic_flat.printSchema()


    println("Resrtucting the flatten columns")

    val re_struct = pic_flat.select(col("id"),
      struct(col("height"),
        col("url"),
        col("width")
      ).as("image"),
      col("name"),
      col("type")
    )

    re_struct.show()
    re_struct.printSchema()








  }

}
