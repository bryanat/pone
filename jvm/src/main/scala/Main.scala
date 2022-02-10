import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine


object Main {

  def Xmain(args: Array[String]): Unit = {
    println("init26")
    
    // Hadoop home directory with winutils to 
    System.setProperty("hadoop.home.dir", "C:\\hadoop")


    val conf = new SparkConf().setMaster("local[2]").setAppName("streamreader").setSparkHome("C:\\Spark")
    //val sc = new SparkContext
    // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    val ssc = new StreamingContext(conf, Seconds(5))
    
  }
}
