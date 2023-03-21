package awspack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object aws_write {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
      .setAppName("first")
      .setMaster("local[*]")
      .set("fs.s3a.access.key", "**************************")
      .set("fs.s3a.secret.key", "**************************************************")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("Hey Coder, Read and Write Data to AWS S3 Bucket")

    println("Reading data from s3")

    val raw_data = spark
      .read
      .format("csv")
      .option("header","true")
      .load("s3a://venu07bucket/usdata.csv")
    raw_data.show()

    raw_data.createOrReplaceTempView("data_tab")

    val la_data = spark.sql("select * from data_tab where state ='LA'")
    la_data.show()

    //la_data.write.format("csv").option("header","true").mode("overwrite").save("s3a://venu07bucket/la_data/")
   // println("success!!")

    println("reading LA data from S3")

    val check_data =spark
      .read.option("header","true")
      .format("csv")
      .load("s3a://venu07bucket/la_data/part-00000-705d4637-2fb4-455b-821e-fe8c79c4df17-c000.csv")
    check_data.show()


  }

}

