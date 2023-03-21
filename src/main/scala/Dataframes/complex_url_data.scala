package Dataframes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import scala.io.Source

object complex_url_data {
     def main(args:Array[String]):Unit={
      val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._


      val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
      val urldata = html.mkString
      println(urldata)
      val urlrdd = sc.parallelize(List(urldata))

      println("=============rdd ============")

      urlrdd.foreach(println)
      val df = spark.read.json(urlrdd)
      df.show()
      df.printSchema()

      val procdf = df.withColumn("results", expr("explode(results)"))

      procdf.show()
      procdf.printSchema()

      val procdf1 = procdf.select (
        "results.user.name.first" ,
        "results.user.name.last"  ,
        "results.user.password"  ,
        "results.user.username")

      procdf1.show()
      procdf1.printSchema()



      println(procdf1.rdd.partitions.size)


      procdf1.coalesce(1)
        .write
        .format("csv")
        .option("header","true")
        .mode("overwrite")
        .save("file:///Users/venu7/Desktop/Zeyobron/Data/urldata")




      println("=== data written ====")




    }



}
