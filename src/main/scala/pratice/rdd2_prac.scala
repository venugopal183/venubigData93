package pratice
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object rdd2_prac {
  def main(Args:String):Unit={
    val conf = new SparkConf().setAppName("RDD Practice").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rdd1 = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/nout/prac.txt")
    rdd1.foreach(println)

  }
}
