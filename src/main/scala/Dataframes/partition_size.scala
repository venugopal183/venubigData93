package Dataframes
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}
object partition_size {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    val df1 = spark.read.format("json").option("multiline","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/file4.json")

    println("printing partition size")
    val partition_size =df1.rdd.partitions.size
    println(partition_size)
    df1.repartition(5)
    val new_partition = df1.repartition(5).rdd.partitions.size
    println(new_partition)


  }

}
