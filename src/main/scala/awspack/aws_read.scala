package awspack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object aws_read {
  def main(args:Array[String]):Unit= {


    val conf = new SparkConf().setMaster("local[*]").setAppName("first").set("fs.s3a.access.key","AKIAULEZTMHLI2ZR4UDF").set("fs.s3a.secret.key","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")



    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    println("reading data from S3 Source")

    val aws_read = spark.read.option("header","true").format("csv").load("s3a://venu07bucket/usdata.csv")
    aws_read.show()

    aws_read.createOrReplaceTempView("awstab")
    val aws_final = spark.sql("select * from awstab where age > 15")
    aws_final.show()



  }

}
