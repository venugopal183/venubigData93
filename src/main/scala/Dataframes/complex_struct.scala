package Dataframes

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object complex_struct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading place.json")

    val raw_data = spark.read
      .format("json")
      .option("multiline","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/place.json")
    raw_data.show()
    raw_data.printSchema()

    println("flattening the data")

    val flat_data = raw_data.select(
      "place",
      "user.address.*",
      "user.name"
    )
    flat_data.show()
    flat_data.printSchema()

    println("reading address.json")
    val raw_add = spark.read.
      format("json")
      .option("multiline","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/address1.json")
    raw_add.show()
    raw_add.printSchema()

    println("flattening address1.json")

    val flat_add1 = raw_add.select("address.billing_address.*",
      "age",
      "date_of_birth",
      "email_address",
      "first_name",
      "height_cm",
      "is_alive",
      "last_name"


    )
    flat_add1.show(false)
    flat_add1.printSchema()


    println("Generating the heirarchial form of address1.json")

    val struct_add1 = flat_add1.select(struct(
      struct(
        col("address"),
        col("city"),
        col("postal_code"),
        col("state")
      ).as("billing_address")

    ).as("age"),
    col("date_of_birth"),
    col("email_address"),
    col("first_name"),
    col("height_cm"),
    col("is_alive"),
    col("last_name")

    )
    struct_add1.show(false)
    struct_add1.printSchema()



  }
}
