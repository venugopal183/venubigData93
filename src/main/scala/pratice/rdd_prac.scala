package pratice
import org.apache.spark.sql.functions.{concat, upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive

object rdd_prac {
  //case class schema_rdd(Id:Int,name:String,City:String,College:String,Gender:String)
  /*def con_mapper(Country:String)= {
    val cols = Country.split(",")
    if (cols(4) == "India") {
      (cols(0), cols(1), cols(2), cols(3), cols(4), "Ind")
    } else if (cols(4) == "Pakistan") {
      (cols(0), cols(1), cols(2), cols(3), cols(4), "PK")
    } else if (cols(4) == "Canada") {
      (cols(0), cols(1), cols(2), cols(3), cols(4), "CAD")
    } else {
      (cols(0), cols(1), cols(2), cols(3), cols(4), "N/A")
    }
  } */
  /*def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]").set("fs.s3a.access.key","******************").set("fs.s3a.secret.key","************************************")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._*/
  def main(Args:String):Unit={
    val conf = new SparkConf().setAppName("Venu practice").setMaster("local[*]")
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val Columns = Seq("Id","Name","City")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("read the file as RDD")
    val df = sc.textFile("file:///").toDF(Columns:_*)

    //df.write.format(hive).mode("append").saveAsTable(db.tab)
    /*println("reading data from aws s3")



    val raw_us_data = spark.read.format("csv").option("header","true").load("s3a://venu07bucket/usdata.csv")
    raw_us_data.show()
    raw_us_data.printSchema()

    println("making changes to the schema")
    val fil_us_data = raw_us_data.withColumn("full_name",concat(col("first_name"),lit(' '),col("last_name"))).drop("first_name","last_name")
    fil_us_data.show()



    val struct_schema = StructType(Array(
      StructField("id",IntegerType,true),
      StructField("Name",StringType,true),
      StructField("City",StringType,true),
      StructField("College", StringType, true),
      StructField("Gender", StringType, true)))

      //case class schema_rdd(Id:Int,name:String,City:String,College:String,Gender:String)

    val raw_df = spark.createDataFrame(List((1,"venu","Toronto","centennial","M"),
      (2,"Bharat","Hyderabad","Nitie","M"),
      (3,"Ripun","California","Uni_Michigan","M"),
      (4,"Rajesh","Germany","PUblic_Uni","M"),
      (5,"Minnu","Toronto","Georgian","F"),
      (6,"Akhil","Toronto","centennial","M"),
      (7,"Jai","Toronto","centennial","M")))
    raw_df.show()

    val ds = spark.catalog.listDatabases()
    ds.show(false)
    val ts = spark.catalog.listTables()
    ts.show() */
    /*val rdd1 = sc.textFile("file:///Users/venu7/Desktop/Zeyobron/nout/prac.txt")
    rdd1.collect().foreach(println)
    println()
    val rdd2 = rdd1.map(line => {
      val fields =line.split(",")
      if(fields(2).toInt >20 && fields(2).toInt <=30){
        (fields(0),fields(1),fields(2),fields(3),fields(4),"Desired")
      } else if(fields(2).toInt >30 && fields(2).toInt <40) {
        (fields(0),fields(1),fields(2),fields(3),fields(4),"Grooming")
      } else {
        (fields(0),fields(1),fields(2),fields(3),fields(4),"Non-Desired")
      }



    })
    rdd2.foreach(println)
    println()
    val rdd3= rdd1.map(con_mapper)
    rdd3.foreach(println)
    //rdd3.saveAsTextFile("file:///Users/venu7/Desktop/Zeyobron/nout/rdd")
    val rdd4 =rdd3.repartition(3)
    //rdd4.saveAsTextFile("file:///Users/venu7/Desktop/Zeyobron/nout/rdd4")
    val rdd5 = rdd3.coalesce(1)
    rdd5.saveAsTextFile("file:///Users/venu7/Desktop/Zeyobron/nout/rdd5")

     */
   /* val lis = Seq(("Venu",1000,150000),("Karthick",1001,120000),("Rick",10002,123435),("George",1003,156000))
    val df = sc.parallelize(lis).toDF("Emp_Name","Id","Salary")
    df.show()
    df.createOrReplaceTempView("emp_tab")
    val df_max_sal= spark.sql("select Emp_Name,Id,Salary from emp_tab where Salary = (select max(Salary) from emp_tab)")
    df_max_sal.show() */




























  }


}
