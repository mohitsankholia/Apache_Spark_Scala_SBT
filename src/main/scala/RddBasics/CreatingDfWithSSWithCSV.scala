package RddBasics

import org.apache.spark.sql.SparkSession

/**
 * @author Mohit Sankholia 
 *         on 18/01/23
 */
object CreatingDfWithSSWithCSV {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("creating spark context with spark session")
      .master("local")
      .getOrCreate()

//    val properties= Map("header" -> "true", "inferSchema" ->"true")

    val df = sparkSession.read
//      .option("header", "true")
      .option("inferSchema", "true")
//      .options(properties)
      .csv("src/Data/SampleCSV.csv")

    df.printSchema()
    df.show()
  }

}
