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
import scala.io.Source
import java.io
import scala.util.control.Breaks._
//import org.saddle._
import scala.util.Random
import simcompare._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, collect_list, lit, when}


//
//  This Version of the program is specifically made for
//  testing the speed of the clique finding approach
//  neibs.csv should be the neighbor list for each node (generated using
//  scaladatagen.py) genData2.csv should just be a list with the number of users
//  and another row that can be whatever. (generated using ppllistgenner.py)
//  other files are not used


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
    // START OF COMMENT OUT FOR CLIQUE TESTING
    /*


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
      //todo change format of dataframe or how it is read in to sim comparison
      val p1 = a.slice(1,a.indexOf('0') - 2).replaceAll("\'","").split(",").map(_.toDouble)
      val t1 = a.slice(a.indexOf('0'), a.lastIndexOf('0') + 2).replaceAll("\'","").split(",").map(_.toDouble)
      val p2 = b.slice(1,b.indexOf('0') - 2).replaceAll("\'","").split(",").map(_.toDouble)
      val t2 = b.slice(a.indexOf('0'), b.lastIndexOf('0') + 2).replaceAll("\'","").split(",").map(_.toDouble)

      //return 0 if index has none in common

      return javasimComp(p1,t1,p2,t2,c)
    }

    val simCompareCall = simCompareCallerfunc _
    val simCompUDF = udf(simCompareCall)

    println("before sim calc")

    val resultsdf = newerDF.withColumn("result", simCompUDF(newerDF("df1traj"), newerDF("traj"), newerDF("lvl")))


    println("after sim calc")

    resultsdf.show()

    println("before combine levels")
    val combineLvlsDf = resultsdf.groupBy("df1ID", "personID").agg(sum("result"))

    println("after combine levels/before above thresh")

    val aboveThreshDf = combineLvlsDf.where(combineLvlsDf("sum(result)") > 1)

    println("after above threshold")
    aboveThreshDf.show()

    val neighborGroups = aboveThreshDf.groupBy("df1ID").agg(collect_list("personID"))

    println("after neighbor groups")

    val neighborGroupsDF = neighborGroups.toDF()
    neighborGroupsDF.show()

    println("start conversions")

    val neighborGroupsArrCol = neighborGroups.toDF().collect
    val neighborGroupsArr = neighborGroupsArrCol.map( r => Array(r.getAs[Int](0), r.getAs[Array[Int]](1)) )
    val Pset = neighborGroupsArr.flatMap(_.headOption).map(x => x.toString.toInt).toSet


    println("after conversion things")

    val neighbors2 = spark.sparkContext.broadcast(neighborGroupsArr)
    val Pbroad = spark.sparkContext.broadcast(Pset)

    */

    val cliqueTestFile = "genData2.csv"


    val cliqueTestDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(cliqueTestFile)

    cliqueTestDF.show()

    def readCSV() : Array[Array[Int]] = {
      scala.io.Source.fromFile("neibs.csv")
        .getLines()
        .map(_.split(",").map(_.trim.toInt))
        .toArray
    }



    val neibgrouparr = readCSV()


    //val neighborGroupsArrCol2 = cliqueTestDF.collect
    //val neighborGroupsArr2 = neighborGroupsArrCol2.map( r => Array(r.getAs[Int](0), r.getAs[Array[Int]](1)) )
    //val Pset2 = neighborGroupsArr2.flatMap(_.headOption).map(x => x.toString.toInt).toSet
    //val Pset2 = (1 to neibgrouparr.length).toSet


    println("after conversion things")

    val neighbors2 = spark.sparkContext.broadcast(neibgrouparr)
    //val Pbroad = spark.sparkContext.broadcast(Pset2)

    def bkcaller(cliqueStart:Any) : Int = {
      //Timing
      val t0 = System.nanoTime()

      val R = Set.empty[Int]
      //val P = (1 to 200).toSet

      val X = Set.empty[Int]
      val csString = cliqueStart.toString.trim
      val cliqueS = Integer.parseInt(csString)
      val filename = csString + "bkresult.txt"
      //val P = Pbroad.value
      //printing removed for testing
      //val writer = new PrintWriter(new File(filename))
      val P = neighbors2.value(cliqueS-1).toSet

      //for testing only print every 1000
      if ( cliqueS % 1000 == 0) {
        println("start of " + cliqueS.toString())
      }

      bkCliqueFinder(R, P, X, cliqueS/*, writer*/)
      //writer.close()

      //val t1 = System.nanoTime()
      //println("Elapsed Time: " + (t1 - t0))

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



    def bkCliqueFinder(R:Set[Int], P:Set[Int], X:Set[Int], cliqueS:Int/*, writer: PrintWriter*/) : Int = {

      if (P.size == 0 && X.size == 0 && R.contains(cliqueS)) {
        if (R.size > 2) {
          //println("Clique: " + R.toString())
          //writer.write(R.toString() + "\n")
        }
      }
      else {
        var Pcopy = P.map(x => x)
        var Rcopy = R.map(x => x)
        var Xcopy = X.map(x => x)

        val pivNeighbs = Set()

        if (!(Pcopy ++ X).toList.isEmpty) {
          val pivot = Random.shuffle((Pcopy ++ X).toList).head
          //originalVersion
          //val pivNeighbsStr = neighbors2.value(findRow(pivot))(1).toString
          //val pivNeighbs = pivNeighbsStr.slice(13,pivNeighbsStr.length-1).split(",").map(_.trim.toInt).toSeq

          //clique testing only
          val pivNeighbs = neighbors2.value(pivot-1).toSeq
          //println(pivNeighbsStr)
          }

        for (vertex <- (Pcopy -- pivNeighbs)) {
          //val neighbs2 = neighbors2.value.get(findRow(vertex))(1)

          //println(neighbs2)
          //original version
          //val neighbsStr = neighbors2.value(findRow(vertex))(1).toString
          //val neighbs = neighbsStr.slice(13,neighbsStr.length-1).split(",").map(_.trim.toInt).toSeq
          //println(neighbs.toString())
          //clique testing only
          val neighbs = neighbors2.value(vertex-1).toSeq


          bkCliqueFinder(R + vertex, Pcopy.intersect(Set(neighbs: _*)), Xcopy.intersect(Set(neighbs: _*)), cliqueS/*, writer*/)
          Pcopy -= vertex
          if (vertex == cliqueS) {
            return 5
          }
          Xcopy = Xcopy + vertex
        }
      }

      return 5
    }


    // END OF COMMENT OUT FOR CLIQUE TESTING



    //val cftest = cliqueFinder _
    val cftest2 = bkcaller _

    //val cfUDF = udf(cftest)
    val cfUDF2 = udf(cftest2)

    // make and show dataframe with column showing the clique starting from each node
    //val ansDF = userDF.withColumn("test", cfUDF(userDF("UID")))


    println("before cliques")
    val ansDF2 = cliqueTestDF.withColumn("test2", cfUDF2(cliqueTestDF("df1ID")) )

    ansDF2.show(1)

    ////
    //END OF COMMENTED OUT OLD PROGRAM
    ////


    spark.stop()
  }
}