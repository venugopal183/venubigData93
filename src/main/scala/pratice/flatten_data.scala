package pratice
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{upper, _}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


object flatten_data {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("Practice").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rcars = spark.read
      .format("json")
      .option("multiline","true")
      .load("file:///Users/venu7/Desktop/Zeyobron/Data/Complex_Data/cars.json")
    rcars.show()
    rcars.printSchema()

    val flat_cars = rcars
      .withColumn("cars",explode(col("cars")))
      .withColumn("models",explode(col("cars.models")))
      .withColumn("car_name",col("cars.name"))
      .drop("cars")

    flat_cars.show()
    flat_cars.printSchema()


    val  act_raw = spark.read.format("json").option("multiline","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/Complex_Data/actorsj.json")
    act_raw.show()
    act_raw.printSchema()

    val flat_actors = act_raw.withColumn("Actors",explode(col("Actors"))).select("Actors.picture.*","Actors.*","country","version").drop("picture")
    flat_actors.show()
    flat_actors.printSchema()

    val  users_raw = spark.read.format("json").option("multiline","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/Complex_Data/array_users.json")
    users_raw.show()
    users_raw.printSchema()

    val flat_users = users_raw.withColumn("results",explode(col("results")))
    .select("nationality","results.user.*","results.user.location.*","results.user.name.*","results.user.picture.*","seed","version").drop("location","name","picture")
    flat_users.show()
    flat_users.printSchema()

    val  batters_raw = spark.read.format("json").option("multiline","true").load("file:///Users/venu7/Desktop/Zeyobron/Data/Complex_Data/batters.json")
    batters_raw.show()
    batters_raw.printSchema()

    val in_batters = batters_raw
      .withColumn("results",explode(col("results")))
      .withColumn("batter",explode(col("results.batters.batter")))
    .withColumn("topping",explode(col("results.topping")))
    .withColumn("batter_id",col("results.batters.batter.id"))
    .withColumn("batter_type",col("results.batters.batter.type"))
      .withColumn("topping_id",col("results.topping.id"))
    .withColumn("topping_type",col("results.topping.type"))
      .withColumn("batter_id",explode(col("batter_id")))
    .withColumn("batter_type",explode(col("batter_type")))
    .withColumn("topping_id",explode(col("topping_id")))
      .withColumn("topping_type",explode(col("topping_type")))
    .select("results.*","batter_id","batter_type","topping_id","topping_type").drop("batters","topping")
    in_batters.printSchema()
    in_batters.show()

    val con_batters = in_batters.withColumn("id",col("id").cast(IntegerType))
      .withColumn("batter_id",col("batter_id").cast(IntegerType))
      .withColumn("topping_id",col("topping_id").cast(IntegerType))

    val anal_batters = con_batters.withColumn("name_list",collect_set(col("name"))).select("name_list")
    anal_batters.show()
  }

}
