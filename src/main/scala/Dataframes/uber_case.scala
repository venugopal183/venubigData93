package Dataframes

import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window


object uber_case {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading uber data")

    val df_uber = spark.read.format("csv").option("header","true").load("/Users/venu7/Desktop/Zeyobron/Data/uber.csv")
    //val fil_data= df_uber.withColumn("ride_date", col("date").cast(DateType)).withColumnRenamed("ride_date",expr(date_format('ride_date')))
    //fil_data.show()
    df_uber.show()

    val finaldf = df_uber.withColumn("Day",expr("DATE_FORMAT(TO_DATE(DATE,'MM/dd/yyyy'),'EEE')"))
      .groupBy("dispatching_base_number","Day")
      .agg(sum("trips").alias("Sum"))
      .withColumn("rnk",rank.over(Window.partitionBy("dispatching_base_number").orderBy("Sum")))
    finaldf.show()

    println

    val int_data = df_uber.withColumn("Day",expr("DATE_FORMAT(TO_DATE(DATE,'MM/dd/yyyy'),'EEE')")).groupBy("dispatching_base_number","Day").agg(sum("trips").as("Total_trips_daywise")).orderBy("Total_trips_daywise")

    int_data.show()

    //fil_data.show()

    /* println("" +
      " convert date to Date Data type   "
      "Convert active_vehicles to int datatype" +
      "Converrting trips to string data type")
    val fil_data = df_uber.withColumn("date",expr("cast('date' as Date)"))
    fil_data.show()*/




  }

}
