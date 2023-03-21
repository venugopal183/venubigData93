package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}

object complex_flattening {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

   /* val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.warehouse.dir", "file:///C:/hivewarehou/")
      .config("spark.sql.catalogImplementation","hive").getOrCreate()
    import spark.implicits._ */
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._


    val df = spark.read.format("json")
      .option("multiline","true")
      .load("file:////Users/venu7/Desktop/Zeyobron/Data/jc6.json")

    df.show()
    df.printSchema()



    val flattendf = df
      .withColumn("Students", expr("explode(Students)"))

    flattendf.show()
    flattendf.printSchema()




    val finalflatten = flattendf.select(

      "Students.user.*",
      "Years",
      "org",
      "trainer"


    )

    finalflatten.show()
    finalflatten.printSchema()





    val complexdf = finalflatten.groupBy("Years","org","trainer")

      .agg(

        collect_list(

          struct(

            struct(
              col("age"),
              col("name")


            ).as("user")

          )

        ).as("Students")

      )

    complexdf.show()
    complexdf.printSchema()

  }



}
