package Streaming
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
object Kinesis_Streaming {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")


    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._


    val ssc = new StreamingContext(conf, Seconds(2))


    val tk = Array("test")


    val kp =

      Map[String, Object](

        "bootstrap.servers" -> "localhost:9092"
        , "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "example22",
        "auto.offset.reset" -> "latest"
      )


    val stream = KafkaUtils
      .createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](
          tk, kp))


    val stream1 = stream.map(x => x.value)

    stream1.print

    stream1.foreachRDD(x =>

      if (!x.isEmpty()) {


        val data = x.map(x => x + ",zeyobron")


        val df = data.toDF("value")

        df.show()


      }


    )


    ssc.start()
    ssc.awaitTermination()

  }

}
