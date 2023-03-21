package Dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._



object Df_read {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("hello Coder, reading different data from various sources using spark")

    val jdf = spark.read.format("json").load("file:////Users/venu7/Desktop/Zeyobron/Data/devices.json")
    jdf.show()
    jdf.dropDuplicates().createOrReplaceTempView("json_tab")
    val final_jdf = spark.sql("select *,row_number() over (order by device_id) rownum from json_tab")
    final_jdf.show()

    println("Reading avro file")

    val avro_raw = spark.read.option("header","true").format("csv").load("file:////Users/venu7/Desktop/Zeyobron/Data/part.avro")
    avro_raw.createOrReplaceTempView("avro_tab")
    val avro_final =spark.sql("select * from avro_tab ")
    avro_final.show()

    println("Reading orc file")

    val raw_orc = spark.read.format("orc").load("file:////Users/venu7/Desktop/Zeyobron/Data/data.orc")
    raw_orc.show()
   val col= raw_orc.columns.toList
    println(col)


   /* println("writing the sorted json and avro files")
    avro_final.write.format("avro").mode("overwrite").save("file:///////Users/venu7/Desktop/Zeyobron/Data/avrowrite")

    final_jdf.write.format("json").mode("overwrite").save("////Users/venu7/Desktop/Zeyobron/Data/jsonwrite")

    raw_orc.write.format("orc").mode("overwrite").save("file:////Users/venu7/Desktop/Zeyobron/Data/orcwrite") */








  }

}
