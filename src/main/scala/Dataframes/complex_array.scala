package Dataframes
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
object complex_array {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val raw_arr = spark.read
      .format("json")
      .option("multiline", "true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/jc-2.json")
    raw_arr.show()
    raw_arr.printSchema()

    println("flattening array type")

    val flat_array = raw_arr.withColumn("Students",explode(col("Students")))
    flat_array.show()
    flat_array.printSchema()

    println("reading pet.json")

    val pet_arr = spark.read
      .format("json")
      .option("multiline", "true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/pets.json")
    pet_arr.show()
    pet_arr.printSchema()

    val flat_pets = pet_arr
      .withColumn("Permanent_address",col("Address.`Permanent address`"))
      .withColumn("current_Address",col("Address.`current Address`"))
      .drop("Address")
      .withColumn("Pets",explode(col("Pets")))

    flat_pets.show()
    flat_pets.printSchema()


  }


}
