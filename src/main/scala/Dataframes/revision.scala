package Dataframes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}

object revision {

  case class schema(txnno:String,
                    txndate:String,
                    custno:String,
                    amount:String,
                    category:String,
                    product:String,
                    city:String,
                    state:String,
                    spendby:String)

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val col_names = List("txnno","txndate","custno","amount","category","product","city","state","spendby")

    println("reading File1 as Schema RDD")
    val raw_data = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/file1.txt")

    val gym_data = raw_data.filter( x => x.contains("Gymnastics"))

    val gym_split = gym_data.map(x=> x.split(","))

    val gym_schema = gym_split.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val df_file1 = gym_schema.toDF()
    df_file1.show()

    println("reading the text file as Row RDD")
    val rdata = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/file2.txt")
    val split_data = rdata.map(x => x.split(","))
    val row_rdd = split_data.map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

    val struct_schema = StructType(Array(
      StructField("txnno",StringType,true),
      StructField("txndate",StringType,true),
      StructField("custno",StringType,true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("spendby", StringType, true)
    ))

    val df_file2 = spark.createDataFrame(row_rdd,struct_schema)
    df_file2.show()

    println("reading data from file3.txt")

    val df_file3 = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/file3.txt")
    df_file3.show()

    println("reading data from file4.json")

    val df_file4 = spark.read
      .format("json")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/file4.json")
      .select(col_names.map(col):_*)
    df_file4.show()

    println("reading data from file5.parquet")
    val df_file5 = spark.read
      .format("parquet")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/file5.parquet")
    df_file5.show()

    println("reading data from file6.xml")
    val df_file6= spark.read
      .format("xml")
      .option("rowtag","txndata")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/file6")
      .select(col_names.map(col):_*)
    df_file6.show()

    println("performing union on all 6 data frames")

    val union_df = df_file1
      .union(df_file2)
      .union(df_file2)
      .union(df_file3)
      .union(df_file4)
      .union(df_file5)
      .union(df_file6)
    union_df.show()
    union_df.printSchema()











  }

}
