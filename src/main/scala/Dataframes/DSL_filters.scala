package Dataframes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import  org.apache.spark.sql._

object DSL_filters {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("local").setMaster("local[*]").set("fs.s3a.access.key", "AKIAULEZTMHLI2ZR4UDF").set("fs.s3a.secret.key", "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")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header","true").load("/Users/venu7/Desktop/Zeyobron/Data/dt.txt")
    df.show()

    println("Performing DSL operations")

    val df_fil = df.filter(col("spendby") === "cash").show()
    val df_mul = df.filter(col("category") === "Gymnastics" && col("spendby") === "cash").show()
    val df_or = df.filter(col("category") === "Gymnastics" || col("spendby") === "cash").show()
    val df_isin = df.filter(col("category") isin("Gymnastics", "Exercise")).show()
    val df_notisn = df.filter(!(col("category") isin ("Gymnastics","Exercise"))).show()
    val df_not = df.filter(!(col("spendby")=== "cash")).show()
    val df_like = df.filter(col("category") like "%Sports%").show()

    val df_selexpr = df.selectExpr("id","split(tdate,'-')[2] as Year","amount","category","UPPER(product)","case when spendby ='cash' then 1 else 0 end as Status").show()
    //df_selexpr.printSchema()

    val df_date = df.selectExpr("id","date_format(to_date(tdate, 'MM-dd-yyyy'), 'yyyy-MM-dd') as field_date","amount","category","product","spendby")
    df_date.show()
    df_date.printSchema()







  }
}

