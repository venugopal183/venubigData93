package pratice
import org.apache.spark.sql.functions.{concat, lit, rand, round, _}
import org.apache.spark.sql.types.{IntegerType, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object random {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RandomCSVGenerator")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val numRows = 100000 // Number of rows in the CSV file
    val numCols = 10 // Number of columns in the CSV file
    val outputPath = "s3://bucket-name/path/to/random.csv" // Output path for the CSV file

    // Generate a DataFrame with random data
    val randomDF = spark.range(numRows).select((0 until numCols).map(_ => rand()): _*)
    randomDF.show()
    randomDF.printSchema()

    // Write the DataFrame as a CSV file
   /* randomDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", ",")
      .csv(outputPath) */

    val firstNames = Seq("John", "Jane", "Bob", "Emily", "Mike", "Sarah")
    val lastNames = Seq("Smith", "Johnson", "Brown", "Lee", "Jones", "Davis")
    val positions = Seq("Manager", "Developer", "Designer", "Accountant", "Sales Representative")
    val departments = Seq("Sales", "Marketing", "Engineering", "Finance", "HR")

    // Set the number of employees to generate
    val numEmployees = 1000

    // Generate employee data
   /* val employees = spark.range(numEmployees)
      .withColumn("first_name", concat(lit(firstNames(Random.nextInt(firstNames.length))), lit(" "), lit(lastNames(Random.nextInt(lastNames.length)))))
      .withColumn("last_name", lit(""))
      .withColumn("email", concat(lit("employee"), $"id", lit("@example.com")))
      .withColumn("position", lit(positions(Random.nextInt(positions.length))))
      .withColumn("department", lit(departments(Random.nextInt(departments.length))))
      .withColumn("salary", round(rand() * 80000 + 40000, 2))
      .withColumn("hire_date", concat(lit("2022-"), round(rand() * 11 + 1, 0).cast("int").cast("string"), lit("-"), round(rand() * 27 + 1, 0).cast("int").cast("string")))*/

    // Write the employee data to a CSV file
    /*employees.write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("s3://bucket-name/path/to/employees.csv")*/

  }
}
