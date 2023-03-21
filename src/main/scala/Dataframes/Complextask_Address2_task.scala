package Dataframes
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object Complextask_Address2_task
{
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reaidng addres2.json")
    val raw_add2 = spark
      .read
      .format("json")
      .option("multiline","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/address2.json")
    raw_add2.show()
    raw_add2.printSchema()




  }

}
