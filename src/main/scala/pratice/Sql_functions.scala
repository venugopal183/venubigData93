package pratice
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}


object Sql_functions {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark =SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("reading files from df local")

    val df = spark
      .read
      .format("csv")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Sql practice/df.csv")
    println("reading files from df1 local")
    val df1 = spark
      .read
      .format("csv")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Sql practice/df1.csv")
    println("reading files from cust local")
    val cust = spark
      .read
      .format("csv")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Sql practice/cust.csv")
    println("reading files from prod local")
    val prod = spark
      .read
      .format("csv")
      .option("header","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Sql practice/prod.csv")
    df.show()
    df1.show()
    cust.show()
    prod.show()

    println("creating temp views on the dataframes to perform sql using operations")

    df.createOrReplaceTempView("t_df")
    df1.createOrReplaceTempView("t_df1")
    cust.createOrReplaceTempView("t_cust")
    prod.createOrReplaceTempView("t_prod")

    println("==== Performing Sql operations on the following temp views using spark.sql")

    spark.sql("select * from t_df order by tdate").show()
    spark.sql("select * from t_df1 order by tdate").show()

    println("selecting columns from tempview")
    spark.sql("select id, tdate,category,amount from t_df").show()
    spark.sql("select id,tdate,amount,spendby from t_df1").show()

    println("applying filter to one column")
    spark.sql("select * from t_df where category ='Exercise'").show()
    spark.sql("select * from t_df1 where tdate ='12-17-2011'").show()

    println("Multi Column Filter")
    spark.sql("select * from t_df where tdate >'05-26-2011' and category ='Exercise' and spendby='cash'").show()
    spark.sql("select * from t_df1 where id >5 and amount = 200").show()

    println("Multi Value filter")

    spark.sql("select * from t_df where tdate in ('02-14-2011','05-26-2011','06-05-2011')").show()
    spark.sql("select * from t_df1 where amount between 200 and 400 ").show()

    println(" sql 'like' filter")
    spark.sql("select * from t_df where category like '%Exercise%'").show()
    spark.sql("select * from t_df where product like '%Gym%'").show()

    println ("not filters")
    spark.sql("select * from t_df where category != 'Exercise'").show()
    spark.sql("select * from t_df where tdate not in ('06-26-2011','06-05-2011')").show()

    println(" null and not null filters")
    spark.sql("select * from t_df where product is null").show()
    spark.sql("select * from t_df1 where product is null").show()

    println("max() and min() functions")
    spark.sql("select max(amount) from t_df").show()
    spark.sql("select min(tdate) from t_df1").show()

    println("Count functions in sql")
    spark.sql("select count(id) from t_df").show()
    spark.sql("select count(product) from t_df where amount >200").show()

    println("condition statement in sql")
    spark.sql("select id, tdate,amount,category,product,spendby, case when spendby='cash' then 1 else 0 end as status, case when amount= 200 then 'low_value' when amount =300 then 'medium_value' when amount > 300 then 'high_value' end as Product_Categoriztion from t_df").show()

    println("concat columns function in sql")
    spark.sql("select id, tdate, concat(category,'-',product) as cat_product, spendby from t_df").show(false)

    println("concat_ws function in sql")
    spark.sql("select id, tdate, concat_ws('-',category,product,spendby) as cat_prod_spendy, amount from t_df").show(false)

    println("using lower, uuper, initcap function in sql")
    spark.sql("select id, tdate, lower(product) as l_product,upper(category) as u_category, initcap(spendby) as Spendby from t_df").show()

    println("Ceil and round Data option")
    spark.sql("select id,amount,ceil(amount),round(amount) from t_df").show()

    println(" replacing null values with 'NA' using coalesce")
    spark.sql("select id, amount, tdate,coalesce(product, 'NA') from t_df ").show()

    println(" trim and distinct function")
    spark.sql("select distinct category,id,tdate, trim(product), amount from t_df").show()

    println("Substring with trim")
    spark.sql("select substring(trim(product),1,10) from t_df").show()

    println("Substring/Split function")
    spark.sql("select substring_index(category,' ',1) from t_df").show()

    println("Union all")
    spark.sql("select * from t_df union all select * from t_df1").show()

    println("union")
    spark.sql("select * from t_df union select * from t_df1 order by id").show()

    println("aggregate functions: sum(),count() using group by")
    spark.sql("select category, sum(amount) as total_income,count(amount) as num_count from t_df group by category").show()

    println("aggregate functions: sum() and count() for grouping multiple columns")
    spark.sql("select category,spendby,sum(amount) as total_income,count(amount) as num_count from t_df group by category,spendby").show()

    println("aggregate function: min() and max()")
    spark.sql("select category, product, max(amount) as max_amount from t_df group by category, product").show()
    spark.sql("select category, product, min(amount) as max_amount from t_df group by category, product").show()

    println("aggregate functions along with group by and order by")
    spark.sql("select category, sum(amount) as total_income,count(amount) as num_count from t_df group by category order by total_income").show()
    println()
    spark.sql("select category, sum(amount) as total_income,count(amount) as num_count from t_df group by category order by num_count").show()
    println()
    spark.sql("select category, sum(amount) as total_income,count(amount) as num_count from t_df group by category order by num_count desc").show()

    println("window function row_number()")
    spark.sql("select category, product, row_number() OVER (partition by category order by amount desc) as row_number from t_df").show()

    println("window function Dense_ran Number")
    spark.sql("select category, product, amount,dense_rank() OVER (partition by category order by amount) as desnse_rank from t_df").show()
    println("window rank() function")
    spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM t_df").show()

    println("having function")
    spark.sql("select category, count(category) as cnt from t_df group by category having cnt >1").show()

    println("window lead function")
    spark.sql("select product, category, lag(amount) OVER(partition by category order by amount) as lead from t_df").show()
    println("window lag function")
    spark.sql("select category,product, amount, lag(amount) OVER (partition by category order by amount) as lag from t_df").show()

    println("inner joins")
    spark.sql("select a.id,a.name, b.product from t_cust a join t_prod b on a.id=b.id ").show()

    println("left join")
    spark.sql("select a.*, b.product from t_cust a left join t_prod b on a.id=b.id ").show()

    println("right join")
    spark.sql("select a.id,a.name, b.product from t_cust a right join t_prod b on a.id=b.id ").show()

    println("full join")
    spark.sql("select a.id, a.name,b.product from t_cust a full join t_prod b on a.id=b.id").show()

    println("left anti join")

    spark.sql("select a.id,a.name from t_cust a LEFT ANTI JOIN t_prod b on a.id=b.id").show()

    println("from_unixtime")
    spark.sql("select id,tdate, from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from t_df").show()

    println("sub-squery in spark sql")

    spark.sql("select sum(amount) as total,con_date from(select id, tdate, from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date, amount,category,product,spendby from t_df) group by con_date").show()

    println("collect_list")
    spark.sql("select category,collect_list(spendby) as col_spend from t_df group by category").show()

    println("collect_set")
    spark.sql("select category, collect_set(spendby) as col_spendby from t_df group by category").show()

    println("explode")
    spark.sql("select category,explode(col_spend) as ex_spend from (select category,collect_set(spendby) as col_spend from t_df group by category)").show()

    println("explode outer")
    spark.sql("select category, explode_outer(col_spend) as ex_spend from (select category, collect_set(spendby) as col_spend from t_df group by category)").show()















































  }

}
