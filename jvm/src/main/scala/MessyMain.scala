import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine



object MessyMain {

  def main(args: Array[String]): Unit = {
    println("init42")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    /*
    val sc = new SparkContext("local[2]", "AppName")
    sc.setLogLevel("ERROR")

    //spark.createDataframe(data).toDF()

    // val file = sc.textFile("input/CountACut.txt")
    // val rdd = file.flatMap(line => line.split(",")).groupByKey()
    // var df = spark1.createDataFrame(rdd).toDF("label", "features")

    // // Create broadcast variable to share data retrieved from StreamingContext to scalajs d3 ???

    */
    

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.
    //val conf = new SparkConf().setMaster("local[2]").setAppName("P1")
    //val ssc = new StreamingContext(conf, Seconds(1))
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("streamreader").setSparkHome("C:\\Spark")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
  
    
    // SparkContext (sc) is main entrypoint for Spark API
  //val sc = new SparkContext("local[2]", "P1")
    // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    // "The associated SparkContext [beneath ssc] can be accessed using ssc.sparkContext" ~ sc
  //val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.sparkContext.setLogLevel("OFF")

    // 1 Define the input sources by creating input DStreams.
    // 2 Define the streaming computations by applying transformation and output operations to DStreams.

    // DStream is a continous series of RDDs
    //var dstream = ssc.textFileStream("C:\\revenant\\pone\\input\\rtdata\\rtstream\\")
    //var dstream = ssc.textFileStream("file:///C:/revenant/pone/input/rtdata/rtstream/")
    var dstream = ssc.textFileStream("file:///C:/revenant/pone/input/rtdata/rtstream")

    // explain .print() method vs println :
      // println() will only run once, when streamingContext is initiated
      // dstream.print() will run every time data comes in
      //println("THISWILLRUNONCE")
      //dstream.print()
      

    // Generates a RDD every 1000 milliseconds
    //var rdd = dstream.compute()

  //val dstream_lines = ssc.textFileStream("input/rtdata/countA/")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    //val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
val dstream_words = dstream.flatMap(_.split(","))




    // Count each word in each batch
    //"The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs," 
    //"which is then reduced to get the frequency of words in each batch of data."
    //"Finally, wordCounts.print() will print a few of the counts generated every second."
val key_coffee_value_1 = dstream_words.map(coffee => (coffee, 1))
val coffeBatchCount = key_coffee_value_1.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    // wordCounts.print()
coffeBatchCount.print()

//List(("coffee1", 200), ("coffee2", 300)).toMap = Map("coffee1"->200, "coffee2"->300)
//val map_from_list = List(("coffee1", 200), ("coffee2", 300)).toMap //Map("coffee1"->200, "coffee2"->300)




    // VectorAssembler > OneHotEncoder > LogisticRegression



/*
    //YCSBGHC
    var rdd: RDD[String, Seq[Double]]= spark1.sparkContext.textFile("input/CountACut.txt").map(line=>line.split(",")).map{case Array(x1, x2)=>(x1, List(x2)).toSeq.groupByKey()}
    var df = rdd.toDF()
    val arrayCol: Column = array(df.columns.drop(1).map(col).map(_.cast(DoubleType)): *)
    df.withColumn("label",when($"features"=== "COLD_cappuccino" or $"features" === "LARGE_cappuccino" or $"features" === "MED_cappuccino" or
     $"features" === "SMALL_cappuccino" or $"features" === "ICY_cappuccino" or $"features" === "Triple_cappuccino" or $"features"=== "Mild_cappuccino", 0.toDouble).otherwise(1.toDouble))
     df.show()
    val result: Dataset[LabeledPoint] = df.select(col("label").cast(DoubleType), arrayCol).map(r => LabeledPoint(
        r.getAs[Double](0),
        Vectors.dense(r.getAs[scala.collection.mutable.WrappedArray[Double]](1).toArray)
      ))
*/    


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate



  }
}
