package awspack

import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession


object aws_task {
  def main(args:Array[String]):Unit={
    val conf= new SparkConf().setAppName("first").setMaster("local[*]").set("fs.s3a.access.key","AKIAULEZTMHLI2ZR4UDF").set("fs.s3a.secret.key","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")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val aws_task = spark.read.format("csv").option("header","true").load("s3a://venu07bucket/dt.txt")
    aws_task.show()

    aws_task.createOrReplaceTempView("tasktab")
    val aws_data = spark.sql("select *,split(tdate,'-')[2] as year,case when spendby='cash' then 1 else 0 end as status from tasktab")
    aws_data.show()

    val fit_data = spark.sql("select *, case when category='Exercise' then 'Fitness' when category='Gymnastics' then 'Kickboxing' when category='Team Sports' then 'Playing' end as Activity from tasktab")
    fit_data.show()

    fit_data.write.format("csv").mode("ignore").save("file:///Users/venu7/Desktop/Zeyobron/datawrite/")




  }


}
