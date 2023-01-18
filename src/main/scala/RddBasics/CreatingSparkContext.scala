package RddBasics

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mohit Sankholia 
 *         on 18/01/23
 */
object CreatingSparkContext {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("First Spark Application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    val array = Array(1,2,3,4,5,6,7,8,9,0)

    val arrayRDD = sc.parallelize(array, 3)

    println("No of elements: ", arrayRDD.count())
//    arrayRDD.foreach(println)
//   arrayRDD.take(3).foreach(println)
  }

}
