import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine
import org.apache.spark.ml.linalg.Vectors


object MessyMain {

  def Zmain(args: Array[String]): Unit = {
    println("init42")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    /*
    //spark.createDataframe(data).toDF()

    // val file = sc.textFile("input/CountACut.txt")
    // val rdd = file.flatMap(line => line.split(",")).groupByKey()
    // var df = dfsc.createDataFrame(rdd).toDF("label", "features")

    // // Create broadcast variable to share data retrieved from StreamingContext to scalajs d3 ???

    */
    
    
    // SparkConf is configuration of Spark Cluster, specifies 2 working threads on local machine and Spark home directory
    val conf = new SparkConf().setMaster("local[2]").setAppName("P1").setSparkHome("C:\\Spark")
    // SparkContext (sc) is main entrypoint for Spark API
    val sc   = new SparkContext(conf)
    // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    val ssc  = new StreamingContext(sc, Seconds(3))
    // Spark log level set to not print INFO lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
    ssc.sparkContext.setLogLevel("WARN")
    // SparkSession is main entrypoint for Spark SQL API
    val dfsc = SparkSession.builder().appName("HiveTest5").config("spark.master", "local").enableHiveSupport().getOrCreate()
    //dfsc.sparkContext.setLogLevel("ERROR")


    // 1 Define the input sources by creating input DStreams.
    // 2 Define the streaming computations by applying transformation and output operations to DStreams.

    // DStream is a discrete stream aka sustained series of RDDs
    var dstream = ssc.textFileStream("file:///C:/revenant/pone/input/rtdata/rtstream")

    // explain .print() method vs println :
      // println() will only run once, when streamingContext is initiated
      // dstream.print() will run every time data comes in
      //println("THISWILLRUNONCE")
      //dstream.print()
      

    // Generates a RDD every 1000 milliseconds
    //var rdd = dstream.compute()

    // Create a DStream that will connect to hostname:port, like localhost:9999
    //val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val dstream_words = dstream.flatMap(_.split(","))


    // Count each word in each batch
    //"The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs," 
    //"which is then reduced to get the frequency of words in each batch of data."
    //"Finally, wordCounts.print() will print a few of the counts generated every second."
    val dstreamPair_key_coffee_value_1 = dstream_words.map(coffee => (coffee, 1))
    val dstreamPair_key_coffee_value_Count = dstreamPair_key_coffee_value_1.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    dstreamPair_key_coffee_value_Count.print()


    //dstreamPair_key_coffee_value_Count.compute()
    dstreamPair_key_coffee_value_Count.foreachRDD((rdd, time) => {
      rdd.collect() // : an array containing all elements in this RDD
      //
    })


/*
    RDD > Array[Double]
    Array[Double]
    DenseVector(Array[Double])
*/
    //RDD
    var x1 = 9; //x1: RDD 




    var daily = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
    // Appends v onto daily
    // daily = daily :+ v
    // var v =  Vectors.dense(x1.toSeq.map(x=>x.toDouble).toVector.toArray)
    // var df = dfsc.createDataFrame(daily.map(Tuple1.apply)).toDF("features")

    // dstreamPair_key_coffee_value_Count.foreachRDD()
    // dstreamPair_key_coffee_value_Count.compute()

    //List(("coffee1", 200), ("coffee2", 300)).toMap = Map("coffee1"->200, "coffee2"->300)
    //val map_from_list = List(("coffee1", 200), ("coffee2", 300)).toMap //Map("coffee1"->200, "coffee2"->300)







// Output // Seq(451, 136, 633, 822, 199, 920, 381, 570, 983, 670, 733, 390, 719, 936, 481)


    // VectorAssembler > OneHotEncoder > LogisticRegression



/*

    //var v =  Vectors.dense(x1.toSeq.map(x=>x.toDouble).toVector.toArray)

    //YCSBGHC
    var rdd: RDD[String, Seq[Double]]= dfsc.sparkContext.textFile("input/CountACut.txt").map(line=>line.split(",")).map{case Array(x1, x2)=>(x1, List(x2)).toSeq.groupByKey()}
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
