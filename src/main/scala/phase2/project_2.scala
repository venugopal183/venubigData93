package phase2
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import scala.io.Source


object project_2 {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("Project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark= SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading avro data from local")

    val raw_avro = spark
      .read
      .format("avro")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/phase2_Project/projectsample.avro")
    raw_avro.show(false)
    raw_avro.printSchema()

    println("reading url data")

    val raw_url = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val raw_string = raw_url.mkString
    val url_rdd = sc.parallelize(List(raw_string))

    val raw_url_df = spark.read.json(url_rdd)
    raw_url_df.show()
    raw_url_df.printSchema()

    println("flattening the url df")

    val url_expl = raw_url_df.withColumn("results",expr("explode(results)"))
    url_expl.show()
    url_expl.printSchema()

    val url_df = url_expl.select("nationality",
      "results.user.location.*",
      "results.user.name.*",
      "results.user.picture.*",
      "results.user.*",
      "seed",
      "version")
      .drop("location","name","picture").drop()
    url_df.show()
    url_df.printSchema()

    println("removing numericals from username")
    val fil_url_data = url_df.withColumn("username",regexp_replace($"username","[0-9]",""))
    fil_url_data.show()
    fil_url_data.printSchema()

    println("performing left broadcast join of raw_avro with fil_url_data")
    val join_data = raw_avro.join(fil_url_data,Seq("username"),"left")
    join_data.show()
    join_data.printSchema()

    println("Splitting the dataframe into two df's based on nationality is null and not null")
    val cust_avail = join_data.filter("nationality is not null")
    cust_avail.show()
    cust_avail.printSchema()

    println()

    val cust_not_avail = join_data.filter("nationality is null")
    cust_not_avail.show()
    cust_not_avail.printSchema()

    val raw_data = spark.read.format("json").option("multiline","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/jc6.json")
    raw_data.show()
    raw_data.printSchema()

    val flat_data = raw_data.withColumn("Students",explode(col("Students")))
    flat_data.show()
    flat_data.printSchema()

    val flatted_data = flat_data.select(col("Students.user.*"),col("Years"),col("org"),col("trainer"))
    flatted_data.show()
    flat_data.printSchema()



















  }

}
