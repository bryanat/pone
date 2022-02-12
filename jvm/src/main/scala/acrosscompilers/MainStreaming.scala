package acrosscompilers

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.PairRDDFunctions._
import scala.collection.mutable.ArrayBuffer

//import acrosscompilers.MainHive


object MainStreaming {
  def init(args: Array[String]): Unit = {
  //def init(): Unit = {
    println("Stopping Hive SparkSession... Starting Spark Streaming StreamingContext...")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    

      val dfsc = SparkSession.builder().appName("HiveApp").config("spark.master", "local").enableHiveSupport().getOrCreate()
      dfsc.sparkContext.setLogLevel("ERROR")
      
      dfsc.sql("CREATE TABLE IF NOT EXISTS BranchABC2(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
      dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BranchABC2")
      dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BranchABC2")
      dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BranchABC2")
      // First generate a list of all 54 beverages 
      var dfres = dfsc.sql("SELECT DISTINCT beverage FROM BranchABC2").collect()
      // Create an empty array, that will end up holding the name of each beverage 
      var beverageArray: ArrayBuffer[String] = ArrayBuffer[String]()
      // Generate an array containing each beverage name 
      dfres.foreach( row => {
        // Add each row to the beverage array after converting it to a string 
        beverageArray += row.get(0).toString()
      })
      beverageArray.length //54 Beverages

      // A little trick to turn an Array into comma separate strings, just want to remember for more complex datatypes that dont have a built in .toDatatype() method 
      // var The54InCSVString = beverageArray.mkString("\",\"")

      // Returns a Map datatype containing all 54 beverages
      var beverageMap = beverageArray.map(beverage => (beverage.toString(), 0)).toMap

      dfsc.stop();

      //BROADCAST beverageMap WITH THE OTHER OBJECTS (could just abstract through an object property) 
      //val broadcastBeverageMap: Broadcast[Map[String,Int]] = dfsc.sparkContext.broadcast(beverageMap)
      //broadcastBeverageMap
    
    
    // SparkConf is configuration of Spark Cluster, specifies 2 working threads on local machine and Spark home directory
    val conf = new SparkConf().setMaster("local[4]").setAppName("P1").setSparkHome("C:\\Spark")
    // SparkContext (sc) is main entrypoint for Spark API
    val sc   = new SparkContext(conf)
    // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    val ssc  = new StreamingContext(sc, Seconds(1))
    // Spark log level set to not print INFO lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
    ssc.sparkContext.setLogLevel("WARN")
    // SparkSession is main entrypoint for Spark SQL API
    //val dfsc = SparkSession.builder().appName("HiveTest5").config("spark.master", "local").enableHiveSupport().getOrCreate()
    //dfsc.sparkContext.setLogLevel("ERROR")
    
    // DStream is a discrete stream aka sustained series of RDDs
    var dstream = ssc.textFileStream("file:///C:/revenant/pone/input/rtdata/artstream")
    
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
    
    // 1 Define the input sources by creating input DStreams.
    // 2 Define the streaming computations by applying transformation and output operations to DStreams.

    // // DStream is a discrete stream aka sustained series of RDDs
    // var dstream = ssc.textFileStream("file:///C:/revenant/pone/input/rtdata/artstream")

    // explain .print() method vs println :
      // println() will only run once, when streamingContext is initiated
      // dstream.print() will run every time data comes in
      //println("THISWILLRUNONCE")
      //dstream.print()
      

    // Generates a RDD every 1000 milliseconds
    //var rdd = dstream.compute()

    // Create a DStream that will connect to hostname:port, like localhost:9999
    //val lines = ssc.socketTextStream("localhost", 9999)

    // // Split each line into words
    // val dstream_words = dstream.flatMap(_.split(","))




    // DStream ADDS RDD TO Map, share Map with ScalaJS
    // DStream generates an RDD of bev_key and bev_value //
    // append the RDD generated by DStream to the global Map variable/object
    // Map("Special_Lite" -> 21, "Cold_Coffee" -> 302)
    //   the global Map variable/object will be shared with the ScalaJS scope


    //var The54Map = Map(Double_cappuccino -> 0, Triple_MOCHA -> 0, LARGE_Lite -> 0, Mild_MOCHA -> 0, Double_LATTE -> 0, Special_MOCHA -> 0, Double_MOCHA -> 0, MED_cappuccino -> 0, Triple_Lite -> 0, Triple_Espresso -> 0, MED_LATTE -> 0, Cold_cappuccino -> 0, ICY_LATTE -> 0, Special_Coffee -> 0, ICY_Lite -> 0, Mild_Lite -> 0, LARGE_Coffee -> 0, Mild_cappuccino -> 0, Special_Espresso -> 0, Special_cappuccino -> 0, Cold_Espresso -> 0, Triple_cappuccino -> 0, MED_MOCHA -> 0, Triple_Coffee -> 0, SMALL_Lite -> 0, Special_Lite -> 0, Cold_MOCHA -> 0, SMALL_MOCHA -> 0, Cold_LATTE -> 0, Double_Coffee -> 0, Special_LATTE -> 0, SMALL_LATTE -> 0, Mild_Coffee -> 0, ICY_MOCHA -> 0, Mild_Espresso -> 0, ICY_Espresso -> 0, SMALL_Coffee -> 0, Cold_Lite -> 0, MED_Espresso -> 0, SMALL_cappuccino -> 0, Double_Lite -> 0, ICY_Coffee -> 0, LARGE_LATTE -> 0, Mild_LATTE -> 0, Double_Espresso -> 0, MED_Lite -> 0, LARGE_MOCHA -> 0, SMALL_Espresso -> 0, LARGE_Espresso -> 0, ICY_cappuccino -> 0, MED_Coffee -> 0, Cold_Coffee -> 0, Triple_LATTE -> 0, LARGE_cappuccino -> 0)

    println(beverageMap)


    //dstreamPair_key_coffee_value_Count.compute()
    dstreamPair_key_coffee_value_Count.foreachRDD((rdd, time) => {
      println(beverageMap)

      //beverageMap += mergevaluesbykey rdd

      //beverageMap

      val xyz = rdd.collect() // : an array containing all elements in this RDD
      println(xyz.mkString(","))

      // beverageMap.map()

      // beverageMap.map[String, Int]( ((String, Int)) => ())

      // beverageMap.map( (beverage: String, currentCount: Int) => {
      //   var newCount = currentCount
      //     (beverage, newCount)
      // } )
      
    })

    // day1.csv
    //(Special_Lite,232),(ICY_LATTE,113),(Cold_cappuccino,1247),(Special_Espresso,633),(LARGE_MOCHA,163),(ICY_Coffee,328),(Mild_cappuccino,998),(SMALL_Espresso,677),(Double_LATTE,667),(SMALL_Lite,1032),(ICY_MOCHA,1047),(LARGE_Espresso,985),(MED_cappuccino,310),(Double_Coffee,42),(Triple_cappuccino,244),(MED_MOCHA,459),(Mild_Coffee,596),(Double_cappuccino,228),(MED_Espresso,105),(Special_cappuccino,922),(Triple_Lite,813),(Triple_LATTE,487),(Mild_MOCHA,503),(LARGE_cappuccino,1022),(Cold_Espresso,334),(Cold_Lite,1025),(SMALL_cappuccino,574)
    // day2.csv
    //(Special_LATTE,886),(Double_Lite,1042),(SMALL_LATTE,538),(Special_MOCHA,196),(LARGE_Lite,108),(SMALL_MOCHA,996),(Mild_cappuccino,467),(Double_MOCHA,621),(SMALL_Coffee,641),(ICY_Espresso,77),(ICY_Lite,292),(Double_Espresso,631),(MED_cappuccino,423),(Triple_cappuccino,208),(LARGE_LATTE,986),(Special_Coffee,597),(Triple_MOCHA,777),(Triple_Coffee,1038),(MED_Coffee,69),(Double_cappuccino,192),(ICY_cappuccino,1825),(Special_cappuccino,503),(Mild_Lite,560),(Mild_Espresso,37),(MED_LATTE,274),(Cold_LATTE,251),(Cold_Coffee,298),(Triple_Espresso,451),(Cold_MOCHA,989),(LARGE_cappuccino,127),(MED_Lite,236),(SMALL_cappuccino,911)
    // day3.csv
    //(Special_Lite,561),(ICY_LATTE,778),(Cold_cappuccino,924),(Special_Espresso,850),(LARGE_MOCHA,72),(ICY_Coffee,41),(Mild_cappuccino,431),(SMALL_Espresso,502),(Double_LATTE,156),(SMALL_Lite,605),(LARGE_Coffee,949),(ICY_MOCHA,256),(MED_cappuccino,387),(Double_Coffee,595),(Triple_cappuccino,741),(MED_MOCHA,200),(Mild_Coffee,1037),(Double_cappuccino,585),(MED_Espresso,238),(Special_cappuccino,467),(Triple_Lite,1002),(Triple_LATTE,172),(Mild_MOCHA,524),(Mild_LATTE,962),(LARGE_cappuccino,91),(Cold_Espresso,215),(Cold_Lite,262),(SMALL_cappuccino,875)



/*
    RDD > Array[Double]
    Array[Double]
    DenseVector(Array[Double])
*/
    //RDD


    var daily = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
    // Appends v onto daily
    // daily = daily :+ v
    // var v =  Vectors.dense(x1.toSeq.map(x=>x.toDouble).toVector.toArray)
    // var df = dfsc.createDataFrame(daily.map(Tuple1.apply)).toDF("features")

    // dstreamPair_key_coffee_value_Count.foreachRDD()
    // dstreamPair_key_coffee_value_Count.compute()

    //List(("coffee1", 200), ("coffee2", 300)).toMap = Map("coffee1"->200, "coffee2"->300)
    //val map_from_list = List(("coffee1", 200), ("coffee2", 300)).toMap //Map("coffee1"->200, "coffee2"->300)



        /*
    //spark.createDataframe(data).toDF()

    // val file = sc.textFile("input/CountACut.txt")
    // val rdd = file.flatMap(line => line.split(",")).groupByKey()
    // var df = dfsc.createDataFrame(rdd).toDF("label", "features")

    // // Create broadcast variable to share data retrieved from StreamingContext to scalajs d3 ???

    */




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
