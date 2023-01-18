package RddBasics

import org.apache.spark.sql.SparkSession

/**
 * @author Mohit Sankholia 
 *         on 18/01/23
 */
object SparkContextWithSparkSession {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("creating spark context with spark session")
      .master("local")
      .getOrCreate()

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

    val arrayRDD = sparkSession.sparkContext.parallelize(array, 3)

    println("No of elements: ", arrayRDD.count())
        arrayRDD.foreach(println)
    //   arrayRDD.take(3).foreach(println)
  }

}
