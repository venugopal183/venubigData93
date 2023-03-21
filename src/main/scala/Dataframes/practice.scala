package Dataframes
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object practice {
 def main(args:Array[String]):Unit={
   println("String Interpolation")
   val name = "Venu"
   val age: Int = 25
   println("Hi "+name+"!\n and the age is "+age+".")
   println(s"Hi $name\n and the age is $age ")
   println(f"Hi $name%s\n and the age is $age%d !!")

   println("Starting if else ")





 }

}
