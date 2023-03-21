package pratice
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType,_}
import org.apache.spark.sql.{SparkSession, types, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window


object interview {
def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("Hey Buddy").setMaster("local[*]").set("fs.s3a.access.key","********************").set("fs.s3a.secret.key","*************************************")
  val sc= new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark= SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

  println("reading the file as RDD")
  val rdd = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/uber.csv")
  rdd.take(5).foreach(println)

  val df= spark.read.option("header","true").csv("file:///Users/venu7/Desktop/Zeyobron/Data/uber.csv")
  df.printSchema()
  df.show(false)

  val df_fil =df.withColumn("Day",expr("date_format(to_date(date,'MM/dd/yyyy'),'EEE')"))
    .groupBy("dispatching_base_number","Day").agg(sum("trips").alias("Total_trips"))
  .withColumn("rnk",dense_rank.over(Window.partitionBy("dispatching_base_number").orderBy("Total_trips")))
  df_fil.show(false)

 println("now performaing windowing function of days")

  val df_days = df.withColumn("Days",date_format(to_date(col("date"),"MM/dd/yyyy"),"EEE"))
  .groupBy("Days","dispatching_base_number").agg(sum("trips").alias("Trips"))
  .withColumn("Rank",dense_rank.over(Window.partitionBy("Days").orderBy("Trips")))

  df_days.show(false)

  val df3= df_days.filter("Rank=2")
  df3.show()

  df_fil.coalesce(1).write.format("csv").save("file:///Users/venu7/Desktop/Zeyobron/Data/uber.csv")
  df_days.coalesce(1).write.save("file:///Users/venu7/Desktop/Zeyobron/Data/uber.csv")







}
}
