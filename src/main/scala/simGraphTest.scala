import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.rdd.RDD

import scala.collection.mutable
//import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import java.io
import scala.util.control.Breaks._
//import org.saddle._

import simcompare._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, collect_list, lit, when}


//this is basically like your class in java
object SimpleApp {
  def main(args: Array[String]): Unit = {


    //suppress log output for testing
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val inPeople = "genPpl.csv"



    //files to be read in for clique finding
    val inFile = "data.csv"
    val inFile2 = "genData2.csv"

    //next lines are to set up the spark application and the spark context
    val conf = new SparkConf().setAppName("testerProg").setMaster("local")
    val spark = SparkSession.builder().appName("simpleApplication").getOrCreate()
    import spark.implicits._


    val pplDF = spark.read
                     .format("csv")
                     .option("header", "true")
                     .load(inPeople)

    pplDF.show()


    //how to make sure neighbors are counted with both IDs groups without having duplicates??
    val newDf = pplDF.select('personID as "df1ID", 'traj as "df1traj").crossJoin(pplDF).where('df1ID =!= 'personID)

    val levels = Seq(1, 2)
    val levelsdf = levels.toDF("lvl")

    val newerDF = newDf.crossJoin(levelsdf)

    //newDf.show()
    newerDF.show()

    val javasimComp = simcompare.simCompareV2.simScorePair _



    def simCompareCallerfunc(a:String, b:String, c:Int) : Double = {

      //formats Dataframe content form into something that can be read by the Java sim compare
      val p1 = a.slice(1,a.indexOf('0') - 2).replaceAll("\'","").split(",").map(_.toDouble)
      val t1 = a.slice(a.indexOf('0'), a.lastIndexOf('0') + 2).replaceAll("\'","").split(",").map(_.toDouble)
      val p2 = b.slice(1,b.indexOf('0') - 2).replaceAll("\'","").split(",").map(_.toDouble)
      val t2 = b.slice(a.indexOf('0'), b.lastIndexOf('0') + 2).replaceAll("\'","").split(",").map(_.toDouble)

      //return 0 if index has none in common

      return javasimComp(p1,t1,p2,t2,c)
    }

    val simCompareCall = simCompareCallerfunc _
    val simCompUDF = udf(simCompareCall)


    val resultsdf = newerDF.withColumn("result", simCompUDF(newerDF("df1traj"), newerDF("traj"), newerDF("lvl")))


    resultsdf.show()

    val combineLvlsDf = resultsdf.groupBy("df1ID", "personID").agg(sum("result"))

    val aboveThreshDf = combineLvlsDf.where(combineLvlsDf("sum(result)") > .8)

    aboveThreshDf.show()

    //collect neighbors not working in rare cases

    val neighborGroups = aboveThreshDf.groupBy("df1ID").agg(collect_list("personID"))

    def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

    //val neighborGroupsArr = neighborGroups.takeAsList(neighborGroups.count().toInt)
    val neighborGroupsDF = neighborGroups.toDF()
    neighborGroupsDF.show()
    val neighborGroupsArrCol = neighborGroups.toDF().collect
    //val neighborGroupsArrCol2 = neighborGroups.toDF().select("df1ID").map(r => r(0)).collect()
    val neighborGroupsArr = neighborGroupsArrCol.map( r => Array(r.getAs[Int](0), r.getAs[Array[Int]](1)) )
    val Pset = neighborGroupsArr.flatMap(_.headOption).map(x => x.toString.toInt).toSet
    //println(neighborGroupsArr.toString())

    //reading in from files using spark
    //val simDF = spark.read
    //                   .format("csv")
    //                   .option("header", "true")
    //                   .load(inFile)

    //val userDF = spark.read
    //                  .format("csv")
    //                  .option("header", "true")
    //                  .load(inFile2)

    //val simRDD: RDD[Row] = simDF.rdd


    //only shows rows where source is less than dest
    //val filtNeighborDF = simDF.filter($"src" < $"dst")

    //groups each source with all of its destinations
    //val neighborsDF = simDF.groupBy("src").agg(collect_list("dst")).show()

    //same as last but with filtered neighbors list
    //val filtgrpNeighbors = filtNeighborDF.groupBy("src").agg(collect_list("dst")).show()

    //hard coded creating the neighbors table to be braodcasted at the moment

    var linecounter = 0

    def readCSV() : Array[Array[Int]] = {
      val bufferedSource = scala.io.Source.fromFile("genData.csv")
      var nbers2dmat :Array[Array[Int]] = Array.empty
      for (line <- bufferedSource.getLines()) {
        linecounter += 1
        val cols = line.split(" ").map(_.trim.toInt)
        nbers2dmat = nbers2dmat :+ cols
      }
      bufferedSource.close()
      return nbers2dmat
    }

    val nbers2dmat2 = readCSV()

    val nbers2dmat3 = Array.ofDim[Array[Int]](linecounter,2)

    for (i <- 0 to linecounter-1) {
      nbers2dmat3(i)(0) = Array(i+1)
      nbers2dmat3(i)(1) = nbers2dmat2(i)
    }

    //nbers2dmat2.foreach( x => println(x.mkString))
    // outputs entire neighbor array
    //println(nbers2dmat3.deep.mkString("\n"))
    //println(nbers2dmat2(0).mkString)
    //println(nbers2dmat2(1)(0))
    //println(nbers2dmat2(0)(1))


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

    //neighborGroups.select(columns: _*).collect.map(_.toSeq)


    val neighbors2 = spark.sparkContext.broadcast(neighborGroupsArr)
    val Pbroad = spark.sparkContext.broadcast(Pset)
    val neighbors = spark.sparkContext.broadcast(nbers2dmat3)

    //println(neighbors2.value)
    //println(neighbors2.value(0)(0))
    //println(neighbors2.value(1)(1))
    val tests = neighbors2.value(1)(1).toString
    val tests2 = tests.slice(13,tests.length-1).split(",").map(_.trim.toInt).toSet
    //println(tests2.mkString)


    def bkcaller(cliqueStart:Any) : Int = {
      //Timing
      val t0 = System.nanoTime()

      val R = Set.empty[Int]
      //val P = (1 to 200).toSet
      val P = Pbroad.value
      val X = Set.empty[Int]
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      val filename = csString + "bkresult.txt"
      val writer = new PrintWriter(new File(filename))

      println("start of " + cliqueS.toString())
      bkCliqueFinder(R, P, X, cliqueS, writer)
      writer.close()

      val t1 = System.nanoTime()
      println("Elapsed Time: " + (t1 - t0))

      return 5
    }


    def findRow(searchInd:Int) : Int = {
      val nSize = neighbors2.value.length
      var rowInd = -1
      for (i <- 0 until nSize) {
        if (neighbors2.value(i)(0).toString == searchInd.toString()) {

          rowInd = i
          return rowInd
        }
      }

      return rowInd
    }


    //rintln(neighbors2.value.get(0)(1).asInstanceOf[Array[Int]])
    //println(neighbors2.value.get(findRow(3)))

    def bkCliqueFinder(R:Set[Int], P:Set[Int], X:Set[Int], cliqueS:Int, writer: PrintWriter) : Int = {

      if (P.size == 0 && X.size == 0 && R.contains(cliqueS)) {
        if (R.size > 2) {
          println("Clique: " + R.toString())
          writer.write(R.toString() + "\n")
        }
      }
      else {
        var Pcopy = P.map(x => x)
        var Rcopy = R.map(x => x)
        var Xcopy = X.map(x => x)

        for (vertex <- Pcopy) {
          //val neighbs2 = neighbors2.value.get(findRow(vertex))(1)

          //println(neighbs2)

          val neighbsStr = neighbors2.value(findRow(vertex))(1).toString
          val neighbs = neighbsStr.slice(13,neighbsStr.length-1).split(",").map(_.trim.toInt).toSeq
          //println(neighbs.toString())

          bkCliqueFinder(R + vertex, Pcopy.intersect(Set(neighbs: _*)), Xcopy.intersect(Set(neighbs: _*)), cliqueS, writer)
          Pcopy -= vertex
          if (vertex == cliqueS) {
            return 5
          }
          Xcopy = Xcopy + vertex
        }
      }

      return 5
    }


    /*
    //actual clique finding function
    //printlns inside used for testing
    def cliqueFinder(cliqueStart:Any) : Array[Int] = {
      //Timing
      val t0 = System.nanoTime()
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
      val t1 = System.nanoTime()
      println("Elapsed Time: " + (t1 - t0))
      return arr
    }


    def cliqueFinderRec(cliqueStart:Any, counter:Int, cliqueArr:ArrayBuffer[Int], writer: PrintWriter) : Array[Int] = {
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      var varcounter = counter
      val maxN = 5
      var flag = true
      var beforelen = 0
      var afterlen = 0
      var nextans = cliqueArr.sorted.toArray
      /*
      if (varcounter > 5) {
        println("Done:")
        println(cliqueArr.toString())
        return cliqueArr.sorted.toArray
      }*/
      println("Start of... " + cliqueS)

      val neighborRow = neighbors.value(cliqueS-1)(1)

      for (neighbor <- neighborRow) {
        //if (neighbor > cliqueS) {
        if (!cliqueArr.contains(neighbor)) {
          //println("current nb " + neighbor)
          flag = true
          for (i <- 0 to varcounter - 1) {
            //println("if " + neighbors.value(neighbor - 1)(1).deep.mkString(", ") + " contains " + cliqueArr(i))
            if (neighbors.value(neighbor - 1)(1).contains(cliqueArr(i))) {
              //println("yes!")
            } else {
              flag = false
              //println("break")
            }
          }
          if (flag == true) {
            val toPass = cliqueArr.clone() += neighbor
            //currentCliqueReport = cliqueArr.clone()
            //println("before rec call")
            beforelen = cliqueArr.length
            //println(cliqueArr.toString())
            //varcounter += 1
            nextans = cliqueFinderRec(neighbor, varcounter + 1, toPass, writer)
            //println("after rec call")
            //println(cliqueArr.toString())
            afterlen = cliqueArr.length
            //println("next result")
            //println(nextans.length)
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
        if ( cliqueArr.length > 2) {
          println(cliqueArr.toString())
          writer.write(cliqueArr.toString())
        }
      }

      return cliqueArr.sorted.toArray

    }
    */

    //val cftest = cliqueFinder _
    val cftest2 = bkcaller _

    //val cfUDF = udf(cftest)
    val cfUDF2 = udf(cftest2)

    // make and show dataframe with column showing the clique starting from each node
    //val ansDF = userDF.withColumn("test", cfUDF(userDF("UID")))

    val ansDF2 = neighborGroupsDF.withColumn("test2", cfUDF2(neighborGroupsDF("df1ID")) )

    //ansDF.show()
    ansDF2.show()

    ////
    //END OF COMMENTED OUT OLD PROGRAM
    ////


    spark.stop()
  }
}