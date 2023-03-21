package awspack
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import  org.apache.spark.sql._

object scala_task {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("local").setMaster("local[*]").set("fs.s3a.access.key","AKIAULEZTMHLI2ZR4UDF").set("fs.s3a.secret.key","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")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("Reading data from AWS S3")

    val raw_data = spark.read.format("csv").option("header","true").load("s3a://venu07bucket/dt.txt")
    raw_data.show()
    raw_data.createOrReplaceTempView("dt_tab")

    println("Task requirement #1")
    val ta_data = spark.sql("select *,case when spendby='cash' then 1 else 0 end as Status from dt_tab")
    ta_data.show()

    println("Task Requirement #2")
    val ta2_data = spark.sql("select category, sum(amount) as Sum from dt_tab group by category")
    ta2_data.show()

    println("Task Requirement #3")
    val ta3_data = spark.sql("select id, split(tdate,'-')[2] as Year,amount,category,product,spendby from dt_tab")
    ta3_data.show()

    println("Task Requirement #4")
    raw_data.dropDuplicates("category").show()






  }

}
