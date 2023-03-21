package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._



object RDD {

  //case class zeyoschema(id:String, Sport:String, Sprot_Platform:String, Winning_Pay_Mode:String)
  //case class pracschema(id:String, Name:String, Technology:String, Environment:String)
def main(args:Array[String]):Unit={
  println("==RDD Operations Start==")
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

 /* println("Reading file datatxns.txt")
  //val datatxns = sc.textFile("file:////Users/venu7/Desktop/Zeyobron/Data/datatxns.txt")
  val usdata = sc.textFile("file:////Users/venu7/Desktop/Zeyobron/Data/usdata.csv")


  val raw_rdd = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/practice.txt")
  val raw_map = raw_rdd.flatMap(x=> x.split(",")).filter(x => x.contains("Spark"))
  println(raw_map.count())

  //val schema_map = raw_map.map(x=> pracschema(x(0),x(1),x(2),x(3)))
  //val df_schema_map = schema_map.toDF()
  //df_schema_map.show()

  val lis1 = Seq((1,"venu","Spark","Hadoop"))
  val lis2 =Seq((2,"Bharat","PGMT","Business_Analyst"))
  val lis3 =Seq((3,"Minnu","Scala","Programming"))
  val lis4 =Seq((4,"Avinash,Spark","Big_data"))
  val lis5 =Seq((5,"Sai","Spark","Trainer"))
  val rdd1 = sc.parallelize(lis1)
  val rdd2 = sc.parallelize(lis2)
  val rdd3 = sc.parallelize(lis3)
  val rdd4 = sc.parallelize(lis4)
  val rdd5 = sc.parallelize(lis5)
  val union_rdd = lis1.union(lis2).union(lis3).union(lis4).union(lis5)
  union_rdd.foreach(println)
  val dis_rdd = union_rdd.distinct
  dis_rdd.foreach(println)

  println()
  val data = spark.sparkContext.parallelize(Seq(("k",5),("s",3),("s",4),("p",7),("p",5),("t",8),("k",6)),3)
  val group = data.groupByKey().collect().mkString(",")
  println(group) */










  //val len_rdd = raw_rdd.map(x=> (x,x.length))
  //len_rdd.foreach(println)

  /*val len_data= usdata.map(x => (x,x.length))
  len_data.foreach(println) */




  /*println("usinf map Split to separate the data")
  val map_data = usdata.map(x => x.split(","))
  println("First look at split data")
  map_data.foreach(println)
  println("Applying Columns to in RDD method for usdata.csv")
  val schema_usdata = map_data.map ( x => zeyoschema(x(0),x(1),x(2),x(3)))
  println("First look at Organised RDD")
  schema_usdata.foreach(println)
  println("Converting RDD to Dataframe")
  val usdata_df = schema_usdata.toDF()
  usdata_df.show()
  print("Writing data frame into Parquet file")

  usdata_df.write.parquet("file:////Users/venu7/Desktop/Zeyobron/Data/uadata.parquet") */














}


}
