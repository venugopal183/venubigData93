package Dataframes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types.{IntegerType, _}


object multiple_joins {
def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._

  println("multiple files for joining")

  println("Started ====>")

  println("===Customer's Data===")

  val rc_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_customers_dataset.csv")
  rc_data.show(10)
  rc_data.printSchema()

  println("===Order's Data===")

  val rorders_items_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_order_items_dataset.csv")
  rorders_items_data.show(10)
  rorders_items_data.printSchema()

  println
  val order_data = rorders_items_data.withColumn("order_price",col("price").cast(FloatType)).drop("price").withColumn("shipping_limit_date", col("shipping_limit_date").cast(TimestampType)).withColumn("freight_value",col("freight_value").cast(FloatType))
  order_data.show(false)
  order_data.printSchema()

  println("===Geo-Location Data===")
  val rgeo_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_geolocation_dataset.csv")
  rgeo_data.show(10)
  rgeo_data.printSchema()

  println("===Order_Payment's Data===")
  val rpay_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_order_payments_dataset.csv")
  rpay_data.show(10)
  rpay_data.printSchema()

  val payments_data= rpay_data.withColumn("payment_installments",col("payment_installments").cast(IntegerType)).withColumn("payment_value",col("payment_value").cast(FloatType)).withColumn("payment_sequential",col("payment_sequential").cast(IntegerType))
  payments_data.show(10,false)
  payments_data.printSchema()
  println

  println("===Order_Review's Data===")
  val rorder_reviews_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_order_reviews_dataset.csv")
  rorder_reviews_data.show(10)
  rorder_reviews_data.printSchema()

  val order_review = rorder_reviews_data.withColumn("review_score",col("review_score").cast(IntegerType)).withColumn("review_creation_date",col("review_creation_date").cast(TimestampType)).withColumn("review_answer_timestamp",col("review_answer_timestamp").cast(TimestampType))
  order_review.show(10,false)
  order_review.printSchema()
  println("===Order_data's Data===")
  val rorder_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_orders_dataset.csv")
  rorder_data.show(10)
  rorder_data.printSchema()
  println
  val orders_data = rorder_data.withColumn("order_purchase_timestamp",col("order_purchase_timestamp").cast(TimestampType))
    .withColumn("order_approved_at",col("order_approved_at").cast(TimestampType))
    .withColumn("order_delivered_carrier_date",col("order_delivered_carrier_date").cast(TimestampType))
    .withColumn("order_delivered_customer_date",col("order_delivered_customer_date").cast(TimestampType))
    .withColumn("order_estimated_delivery_date",col("order_estimated_delivery_date").cast(TimestampType))
  orders_data.show(10,false)
  orders_data.printSchema()
  println

  println("===Product's Data===")
  val rproducts_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_products_dataset.csv")
  rproducts_data.show(10)
  rproducts_data.printSchema()

  val product_data = rproducts_data.withColumn("product_name_lenght",col("product_name_lenght").cast(IntegerType))
    .withColumn("product_description_lenght",col("product_description_lenght").cast(IntegerType))
    .withColumn("product_photos_qty",col("product_photos_qty").cast(IntegerType))
    .withColumn("product_weight_g",col("product_weight_g").cast(IntegerType))
    .withColumn("product_length_cm",col("product_length_cm").cast(IntegerType))
    .withColumn("product_height_cm",col("product_height_cm").cast(IntegerType))
    .withColumn("product_width_cm",col("product_width_cm").cast(IntegerType))
    .withColumnRenamed("product_description_lenght","product_description_length").
    withColumnRenamed("product_name_lenght","product_name_length")

  product_data.show(10,false)
  product_data.printSchema()

 println
  println("===Seller's's Data===")
  val rseller_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/olist_sellers_dataset.csv")
  rseller_data.show(10,false)
  rseller_data.printSchema()

  println("===Product_Category Data===")
  val rpro_cat_data = spark.read.format("csv").option("header","true").load("file:///Users/venu7/Desktop/Zeyobron/Kaggle_Data/product_category_name_translation.csv")
  rpro_cat_data.show(10,false)
  rpro_cat_data.printSchema()

  println("Joining customer data and payment data")

  val rjoin_df = order_data.join(payments_data,Seq("order_id"),"inner").join(order_review,Seq("order_id"),"inner")
  rjoin_df.show(false)
  rjoin_df.count()

  println("Total sales by state")














}
}
