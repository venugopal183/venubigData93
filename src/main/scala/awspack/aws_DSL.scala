package awspack


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object aws_DSL {

  def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("fs.s3a.access.key","AKIAULEZTMHLI2ZR4UDF").set("fs.s3a.secret.key","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")
  val sc= new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

  /*println("Importing Data from AWS")

  val rdata = spark.read.
    format("csv").option("header","true").
    load("s3a://venu07bucket/usdata.csv")
  rdata.show() */

    println("new code")
    val col = Seq("id","Name","Place")
    val data = Seq (("1","Venu","Hyderabad"),("2","Bharat","KNR"))
    /*val rdatata = sc.parallelize(data)
    val df = spark.createDataFrame(data).toDF(col:_*)
    df.show() */







  }
}
