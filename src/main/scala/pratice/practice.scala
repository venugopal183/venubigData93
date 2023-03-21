package pratice
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}

object practice {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading complex data for assignment task")
    val lis_data = Seq((1,"2020-12-30T09:00:00","IN"),
    (1,"2020-12-30T13:00:00","OUT"),
    (1,"2020-12-30T14:00:00","IN"),
    (1,"2020-12-30T17:00:00","OUT"),
    (1,"2020-12-30T17:30:00","IN"),
    (1,"2020-12-30T20:00:00","OUT"),
    (1,"2020-12-30T20:00:00","OUT"),
    (2,"2020-12-30T10:30:00","IN"),
    (2,"2020-12-30T15:00:00","OUT"),
    (2,"2020-12-30T16:30:00","IN"),
    (2,"2020-12-30T21:30:00","IN")
    )
    val raw_sc = sc.parallelize(lis_data)
    val raw_df = spark.createDataFrame(raw_sc).toDF("emp_id","t_s","Status")
    raw_df.show()
    raw_df.printSchema()

    println("changes in schema")

    val fil_data= raw_df.withColumn("t_s", col("t_s").cast(TimestampType))
    fil_data.printSchema()
    fil_data.show(false)









  }

}
