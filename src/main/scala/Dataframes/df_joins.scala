package Dataframes
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

object df_joins {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark =SparkSession.builder.getOrCreate()
    import spark.implicits._

    println("Starting Joins Session Using J11 and J22")
    val df1 = spark.read.format("csv").option("header","true").load("/Users/venu7/Desktop/Zeyobron/Data/j11.csv")
    df1.show()

    println("reading data from j22")

    val df2 = spark.read.format("csv").option("header","true").load("/Users/venu7/Desktop/Zeyobron/Data/j22.csv")
    df2.show()
    df1.createOrReplaceTempView("df1_tab")
    df2.createOrReplaceTempView("df2_tab")
    val df_in = spark.sql("select a.*,b.product from df1_tab a INNER JOIN df2_tab b on a.id = b.id")
    df_in.show()

    val df_right = spark.sql("select b.*,a.name from df1_tab a  RIGHT JOIN df2_tab b on a.id =b.id")
    df_right.show()

    val df_left = spark.sql("select a.*, b.product from df1_tab a LEFT JOIN df2_tab b on a.id=b.id")
    df_left.show()

    val df_full = spark.sql ("select a.*, b.* from df1_tab a FULL OUTER JOIN df2_tab b on a.id =b.id")
    df_full.show()

   /* println("inner join")

    val df_in = df1.join(df2,Seq("id"),"inner")
    df_in.orderBy("id").show()

    println("outer join")
    val df_out = df1.join(df2, Seq("id"),"outer")
    df_out.orderBy("id").show()

    println("left join")
    val df_left = df1.join(df2,Seq("id"),"left")
    df_left.orderBy("id").show()

    println("right join")
    val df_right = df1.join(df2,Seq("id"),"right")
    df_right.orderBy("id").show()

    println("left_anti")
    val df_lanti = df1.join(df2,df1("id")===df2("id"),"left_anti")
    df_lanti.orderBy("id").show()

    println("full join")
    val df_full_join = df1.join(df2,Seq("id"),"full")
    df_full_join.show()

    println("cross join")

    val df_cjoin = df1.crossJoin(df2)
    df_cjoin.show() */


    println("Task Finished !!!")

  }

}
