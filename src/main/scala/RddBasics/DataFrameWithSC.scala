package RddBasics

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Mohit Sankholia 
 *         on 18/01/23
 */
object DataFrameWithSC {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("First Spark Data Frame Application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val rdd =  sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))

    val schema = StructType(StructField("Numebers", IntegerType, false) :: Nil)

    val rowRdd = rdd.map(line => Row(line))

    val df = sqlContext.createDataFrame(rowRdd, schema)

    df.printSchema()
    df.show()

  }

}
