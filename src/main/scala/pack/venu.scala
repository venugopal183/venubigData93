package pack

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object venu {
def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  println("Hello Coder")
  val data = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/scdata.txt")
  data.foreach(println)
  val lis = List("BigData-Spark-Hive","Spark-Hadoop-Hive","Sqoop-Hive-Spark","Sqoop-BD-Hive")
  val flat_lis = lis.flatMap(x => x.split("-")).distinct
  println("==Flattened Data==")
  val app_lis = flat_lis.map (x => "Tech-> "+x+"Trainer-> Sai")
  val sorted= app_lis.map(x => x.replace("BD","Big Data"))
  sorted.foreach(println)
  val data_1 = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/datatxns.txt")
  val gym_data = data_1.filter( x => x.contains("Gymnastics"))
  gym_data.foreach(println)

}
}
