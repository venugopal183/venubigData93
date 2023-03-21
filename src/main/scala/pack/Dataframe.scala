package pack
import org.apache.spark.sql.{SparkSession,types,functions,_}
import org.apache.spark.{SparkConf, SparkContext}

object Dataframe {
  case class trialscehma(id:String,Product:String,Cost:String,Payment_Mode:String)
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ss = SparkSession.builder().getOrCreate()
    import ss.implicits._
    println("Hello Coder")
    val rawdata = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/Data/datatxns.txt")
    val splitdata = rawdata.map(x => x.split(","))
    val map_data= splitdata.map(x => trialscehma(x(0),x(1),x(2),x(3)))
    val data_df = map_data.toDF()
    data_df.show()






  }

}
