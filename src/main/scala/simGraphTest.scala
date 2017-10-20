import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val inFile = "data.csv"
    val inFile2 = "data2.csv"
    val conf = new SparkConf().setAppName("testerProg").setMaster("local")
    val spark = SparkSession.builder().appName("simpleApplication").getOrCreate()
    val spark2 = spark
    import spark2.implicits._

    val simDF = spark.read
                       .format("csv")
                       .option("header", "true")
                       .load(inFile)

    val userDF = spark.read
                      .format("csv")
                      .option("header", "true")
                      .load(inFile2)

    val simRDD: RDD[Row] = simDF.rdd

    def testudf(a:Any, b:Any) : Int = {
      val stringA = a.toString.trim
      val stringB = b.toString.trim
      val combo = Integer.parseInt(stringA) + Integer.parseInt(stringB)
      return combo
    }



    val testF = testudf _

    val testUDF = udf(testF)

    val mappedRDD = simRDD.map(row => testudf(row.get(0), row.get(1)))

    val udfDF = simDF.withColumn("test", testUDF(simDF("src"), simDF("dst"))).show()

    val filtNeighborDF = simDF.filter($"src" < $"dst")

    val neighborsDF = simDF.groupBy("src").agg(collect_list("dst")).show()

    val filtgrpNeighbors = filtNeighborDF.groupBy("src").agg(collect_list("dst")).show()

    val neighbors = spark.broadcast(1)

    spark.stop()
  }
}