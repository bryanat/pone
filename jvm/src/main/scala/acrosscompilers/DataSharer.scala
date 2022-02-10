package acrosscompilers

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine



object DataSharer {

  def shareData(args: Array[String]): Unit = {
    println("init66")

    val sc = new SparkContext("local[2]", "VectorMaker")
    sc.setLogLevel("ERROR")

    

  }
}
