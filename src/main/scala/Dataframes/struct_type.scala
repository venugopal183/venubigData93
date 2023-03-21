package Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object struct_type {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("fs.s3a.access.key","********************").set("fs.s3a.secret.key","*****************************")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("Reading data from AWS")

    val new_schema = StructType(Array(
      StructField("firstname",StringType,true),
      StructField("middlename",StringType,true),
      StructField("lastname",StringType,true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val sc_data = spark.read.format("csv").schema(new_schema).load("s3a://venu07hdsdbucket/schemadata.csv")
    sc_data.show()








  }




}
