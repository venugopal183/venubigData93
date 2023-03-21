package awspack

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object aws_pratice {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first")
      .setMaster("local[*]")
      .set("fs.s3a.access.key","AKIAULEZTMHLI2ZR4UDF")
      .set("fs.s3a.secret.key","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")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("reading data from AWS Started !!!")
    val df = spark.read.format("csv").option("header","true").load("s3a://venu07bucket/usdata.csv")
    val df_fil = df.withColumn("first_name",expr("UPPER(first_name)"))
      .withColumnRenamed("first_name","Upper_First_name")
    df_fil.show()



  }

}
