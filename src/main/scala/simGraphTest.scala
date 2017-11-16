import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import java.io
import scala.util.control.Breaks._
//import org.saddle._

//there are a lot of imports for random stuff... some
//were added during testing random stuff and could probably be removed


//this application object is what the entire program
object SimpleApp {
  def main(args: Array[String]): Unit = {

    //files to be read in
    val inFile = "data.csv"
    val inFile2 = "data2.csv"

    //next lines are to set up the spark application and the spark context
    val conf = new SparkConf().setAppName("testerProg").setMaster("local")
    val spark = SparkSession.builder().appName("simpleApplication").getOrCreate()
    import spark.implicits._

    //reading in from files using spark
    val simDF = spark.read
                       .format("csv")
                       .option("header", "true")
                       .load(inFile)

    val userDF = spark.read
                      .format("csv")
                      .option("header", "true")
                      .load(inFile2)

    val simRDD: RDD[Row] = simDF.rdd


    //test user defined function that combines rows as strings to the output row
    def testudf(a:Any, b:Any) : Int = {
      val stringA = a.toString.trim
      val stringB = b.toString.trim
      val combo = Integer.parseInt(stringA) + Integer.parseInt(stringB)
      return combo
    }


    //to declare function as a spark userdefined function
    val testF = testudf _
    val testUDF = udf(testF)

    //sends col 0 and col 1 to the test udf as a map (can ignore)
    val mappedRDD = simRDD.map(row => testudf(row.get(0), row.get(1)))
    //creates a new column in the dataframe by sending the src and dst cols
    val udfDF = simDF.withColumn("test", testUDF(simDF("src"), simDF("dst"))).show()
    //only shows rows where source is less than dest
    val filtNeighborDF = simDF.filter($"src" < $"dst")
    //groups each source with all of its destinations
    val neighborsDF = simDF.groupBy("src").agg(collect_list("dst")).show()
    //same as last but with filtered neighbors list
    val filtgrpNeighbors = filtNeighborDF.groupBy("src").agg(collect_list("dst")).show()

    //hard coded creating the neighbors table to be braodcasted at the moment
    val nbers2dmat = Array.ofDim[Array[Int]](6,2)
    nbers2dmat(0)(0) = Array(1)
    nbers2dmat(1)(0) = Array(2)
    nbers2dmat(2)(0) = Array(3)
    nbers2dmat(3)(0) = Array(4)
    nbers2dmat(4)(0) = Array(5)
    nbers2dmat(5)(0) = Array(6)
    nbers2dmat(0)(1) = Array(2,3,4,5,6)
    nbers2dmat(1)(1) = Array(1,5)
    nbers2dmat(2)(1) = Array(1,4)
    nbers2dmat(3)(1) = Array(1,3)
    nbers2dmat(4)(1) = Array(1,2)
    nbers2dmat(5)(1) = Array(1)

    val neighbors = spark.sparkContext.broadcast(nbers2dmat)

    def bkcaller(cliqueStart:Any) : Int = {
      val R = Set.empty[Int]
      val P = Set(1,2,3,4,5,6)
      val X = Set.empty[Int]

      bkCliqueFinder(R, P, X)

      return 5
    }

    def bkCliqueFinder(R:Set[Int], P:Set[Int], X:Set[Int]) : Int = {
      if (P.size == 0 && X.size == 0) {
        println("Clique: " + R.toString())
      }
      else {
        var Pcopy = P.map(x => x)
        var Rcopy = R.map(x => x)
        var Xcopy = X.map(x => x)

        for (vertex <- Pcopy) {
          val neighbs = neighbors.value(vertex-1)(1)

          bkCliqueFinder(R + vertex, Pcopy.intersect(Set(neighbs: _*)), Xcopy.intersect(Set(neighbs: _*)))
          Pcopy -= vertex
          Xcopy = Xcopy + vertex
        }
      }

      return 5
    }



    //actual clique finding function
    //printlns inside used for testing
    def cliqueFinder(cliqueStart:Any) : Array[Int] = {
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      val cliqueArr = ArrayBuffer[Int]()
      val cliqueReport = ArrayBuffer[Int]()
      val filename = csString + "result.txt"
      val writer = new PrintWriter(new File(filename))
      cliqueArr += cliqueS

      println("*******************Start of " + cliqueS + "*********")

      val arr = cliqueFinderRec(cliqueStart, 1, cliqueArr, writer)
      writer.close()
      return arr
    }

    /*def cliqueFinderRec(cliqueStart:Any) : Array[Int] = {
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      var cliqueArr = ArrayBuffer[Int]()
      cliqueArr += cliqueS
      var counter = 1
      val maxN = 4
      var flag = true
      val neighborRow = neighbors.value(cliqueS - 1)
      for (neighbor <- neighborRow(1)) {
        //if (neighbor > cliqueS) {
        println("current nb " + neighbor)
        flag = true
        for (i <- 0 to counter - 1) {
          println("if " + neighbors.value(neighbor - 1)(1).deep.mkString(", ") + " contains " + cliqueArr(i))
          if (neighbors.value(neighbor - 1)(1).contains(cliqueArr(i))) {
            // do nothing
          } else {
            flag = false
          }
        }
        if (flag == true) {
          cliqueArr += neighbor
          counter += 1
        }
        //}
      }

      println(cliqueArr.toString())

      return cliqueArr.sorted.toArray

    }*/

    def cliqueFinderRec(cliqueStart:Any, counter:Int, cliqueArr:ArrayBuffer[Int], writer: PrintWriter) : Array[Int] = {
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      var varcounter = counter
      val maxN = 6
      var flag = true
      var beforelen = 0
      var afterlen = 0
      var nextans = cliqueArr.sorted.toArray

      if (varcounter > 6) {
        println("Done:")
        println(cliqueArr.toString())
        return cliqueArr.sorted.toArray
      }
      println("Start of... " + cliqueS)

      val neighborRow = neighbors.value(cliqueS-1)(1)

      for (neighbor <- neighborRow) {
        //if (neighbor > cliqueS) {
        if (!cliqueArr.contains(neighbor)) {
          println("current nb " + neighbor)
          flag = true
          for (i <- 0 to varcounter - 1) {
            println("if " + neighbors.value(neighbor - 1)(1).deep.mkString(", ") + " contains " + cliqueArr(i))
            if (neighbors.value(neighbor - 1)(1).contains(cliqueArr(i))) {
              println("yes!")
            } else {
              flag = false
              println("break")
            }
          }
          if (flag == true) {
            val toPass = cliqueArr.clone() += neighbor
            //currentCliqueReport = cliqueArr.clone()
            println("before rec call")
            beforelen = cliqueArr.length
            println(cliqueArr.toString())
            //varcounter += 1
            nextans = cliqueFinderRec(neighbor, varcounter + 1, toPass, writer)
            println("after rec call")
            println(cliqueArr.toString())
            afterlen = cliqueArr.length
            println("next result")
            println(nextans.length)
            //cliqueArr.trimEnd(1)
          }
        }
      }

      //println("complete clique")
      //println(cliqueArr.toString())
      if (beforelen == 0 && nextans.length > beforelen) {
        //println(beforelen)
        //println(afterlen)
        //println(nextans.length)
        println("THIS IS A REAL CLIQUE")
        println(cliqueArr.toString())
        writer.write(cliqueArr.toString())

      }

      return cliqueArr.sorted.toArray

    }

    val cftest = cliqueFinder _
    val cftest2 = bkcaller _

    val cfUDF2 = udf(cftest2)

    val cfUDF = udf(cftest)
    // make and show dataframe with column showing the clique starting from each node
    val ansDF = userDF.withColumn("test", cfUDF(userDF("UID")))

    val ansDF2 = userDF.withColumn("test2", cfUDF2(userDF("UID")) )

    ansDF.show()
    ansDF2.show()

    spark.stop()
  }
}