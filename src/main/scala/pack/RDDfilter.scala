package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}


object RDDfilter {
  case class fileschema(id:String,Sport:String,Stage:String,payment_mode:String)
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val spa = SparkSession.builder().getOrCreate()
    import spa.implicits._
    val rawdata = sc.textFile("file:////Users/venu7/Desktop/Zeyobron/Data/datatxns.txt")
    println("Splitting the data")
    val data = rawdata.map(x => x.split(","))
    data.foreach(println)
    val schema_data = data.map(x => fileschema(x(0),x(1),x(2),x(3)))
    schema_data.foreach(println)
    val fil_data = schema_data.filter(x => x.Sport.contains("Gymnastics") && x.id.toInt >25)
    val df = fil_data.toDF()
    df.show()










  }
}
