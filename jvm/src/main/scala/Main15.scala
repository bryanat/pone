import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer

//import org.apache.spark.ml._


object Main15 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val dfsc = SparkSession.builder().appName("HiveTest5").config("spark.master", "local").enableHiveSupport().getOrCreate()
    dfsc.sparkContext.setLogLevel("ERROR")
    
    
    dfsc.sql("CREATE TABLE IF NOT EXISTS BranchABC(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BranchABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BranchABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BranchABC")
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountABC(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' OVERWRITE INTO TABLE CountABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE CountABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE CountABC")
    
    dfsc.sql("SELECT * FROM BranchABC").show()
    dfsc.sql("SELECT * FROM CountABC").show()
    dfsc.sql("SELECT DISTINCT beverage FROM BranchABC").show()


    def init() = {
      def menu(): Unit = {
        println("""
          |
          |
          |== MENU == 
          |Select a number to choose a scenario
          |0. Exit
          |1. Scenario 1 - Beverages Branch Counts
          |2. Scenario 2 - Beverages Max Min Mean
          |3. Scenario 3 - Beverages Available
          |4. Scenario 4 - Partition
          |5. Scenario 5 - Note and Remove
          |6. Scenario 6 - Future Query
          |7. Login
          |""".stripMargin) // stripMargin removes padding from the left side of the console
        var selection = 99 // default value (non 0-5)
        try {
          selection = readLine("Select a number from the menu: ").toInt
        }
        catch {
          case e : Throwable => println("Pick again from the menu.")
        }
        finally {
          selection match {
            case 0 => System.exit(0)            // 0 is exit
            case 1 => {scenario1(); menu()}    // .insert()
            case 2 => {scenario2(); menu()}    // .find()
            case 3 => {scenario3(); menu()}    // .update()
            case 4 => {scenario4(); menu()}    // .delete()
            case 5 => {scenario5(); menu()}  // .find() key value
            case 6 => {scenario6(); menu()}    // .find() but async
            case _ => {println("Invalid number, pick a number from the menu."); menu()} // catch all for non numbers 0-7, recursive menu() call to loop 
          }
        }
      }
      menu() 
    }
    init()



    def scenario1(): Unit = {
      // Problem Scenario 1 
      // What is the total number of consumers for Branch1?
      // What is the number of consumers for the Branch2?

      // Method used was Type 1: a single table with sub queries.
      // over the method Type 2: multiple tables which consume more memory in Spark.
      // the main downside to Type 1 is query performance, but if you wanted to optimize queries you could partition the table

    }

    def scenario2(): Unit = {
      // Problem Scenario 2 
      // What is the most consumed beverage on Branch1 max()
      // What is the least consumed beverage on Branch2 min()
      
      // What is the Average consumed beverage of  Branch2 avg()
      // QUESTION #2 Average can be interpretted in 2 ways: mean and median: mean of each beverage, median of overall table
      // Mean
      dfsc.sql("CREATE TABLE IF NOT EXISTS SUM_BEVERAGES AS SELECT CountA.beverages, sum(CountA.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages")
      // Add Row column
      dfsc.sql("CREATE TABLE IF NOT EXISTS NEW SELECT *, ROW_NUMBER() OVER (ORDER BY beverage_sum) as row FROM SUM_BEVERAGES")
      // Median
      dfsc.sql("SELECT beverages as average_consumed_beverage from NEW where row=round((count(*)/2), 0)").show()
    

      def questionTwo(): Unit = {
      var input = readLine("input a menu number")
      if (input=="1") {
        var b = "Branch"+input
        //prints out two tables, first showing branch intersections
        //second is the top 3 of most consumed
        dfsc.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')")
        dfsc.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        dfsc.sql("SELECT CountA.beverages, sum(CountA.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages order by beverage_sum desc").show(3)
        /*
        //#2
        ("select CountA.beverages, sum(CountA.count) as beverage_sum from BranchesA Join CountA on BranchesA.beverages=CountA.beverages where BranchesA.branches='Branch1' group by CountA.beverages order by beverages_sum desc ")
      
        dfsc.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')").show(60, false)
        dfsc.sql("SELECT sum(CountA.count) as Total_Consumers FROM  BranchesA JOIN CountA ON BranchesA.beverages= CountA.beverages WHERE BranchesA.branches = 'Branch1'").show()

        */
      }
    }
        
    }

    def scenario3(): Unit = {
      // Problem Scenario 3
      // What are the beverages available on Branch10, Branch8, and Branch1?
      // what are the comman beverages available in Branch4,Branch7?

       // QUESTION #3 MAYBE USE collect_set(branch) IN HIVE QUERY

    }
      
    def scenario4(): Unit = {
      // Problem Scenario 4
      // create a partition,View for the scenario3.
      // var b1 = "Branch"+inputBranch(0)
      // var b2 = "Branch"+inputBranch(1)
      // dfsc.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches = '$b2'")
      // dfsc.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
      // dfsc.sql(s"SELECT DISTINCT beverages FROM (SELECT beverages, collect_set(branches) as b FROM Partitioned group by beverages) WHERE ARRAY_CONTAINS(b, '$b1') AND ARRAY_CONTAINS(b, '$b2')").show()
      // dynamic partition: dfsc.sql("CREATE TABLE IF NOT EXISTS Partitioned(beverages STRING) COMMENT 'A PARTITIONED BRANCH TABLE' PARTITIONED BY (branches STRING)")
      // dfsc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      // dfsc.sql("INSERT OVERWRITE TABLE Partitioned PARTITION(branches) SELECT beverages,branches from Branches")
      // dfsc.sql("SELECT * FROM Partitioned")
      // ssql.sql("SHOW PARTITIONS BranchTablePartitioned").show()
    }
    
    def scenario5(): Unit = {
      // Problem Scenario 5
      // Alter the table properties to add "note","comment"
      // Problem Scenario 5: Alter the table properties to add "note","comment"
      // notes = Comment
      // SHOW TBLPROPERTIES <tablename>
      dfsc.sql("ALTER TABLE TableToManipulate SET TBLPROPERTIES ('notes' = 'NOTE THIS TABLE WILL BE EDITED FREQUENTLY')")
      dfsc.sql("SHOW TBLPROPERTIES TableToManipulate").show()
      // DESCRIBE FORMATTED table_name

      // Remove a row from the any Senario.
      def deleteRowNoParams(): Unit = {
      //create copy table
      dfsc.sql("CREATE TABLE IF NOT EXISTS newone2_copy LIKE newone2")
      //load data into copy table except deleted item
      dfsc.sql("INSERT INTO newone2_copy SELECT * FROM newone2 WHERE name NOT IN (SELECT name FROM newone2 WHERE name='varun')")
      //overwrite copy table to original table
      dfsc.sql("INSERT OVERWRITE TABLE newone2 SELECT * FROM newone2_copy")
      //drop copy table
      dfsc.sql("DROP TABLE newone2_copy")
      //show new table with deleted row
      dfsc.sql("SELECT * FROM newone2").show(5)
    }

    def deleteRow(table: String, key: String, value: String): Unit = {
      val table_copy: String = table+"_copy"
      //create copy table
      dfsc.sql(s"CREATE TABLE IF NOT EXISTS $table_copy LIKE newone2")
      //load data into copy table except deleted item
      dfsc.sql(s"INSERT INTO $table_copy SELECT * FROM $table WHERE $key NOT IN (SELECT $key FROM $table WHERE $key='$value')")
      //overwrite copy table to original table
      dfsc.sql(s"INSERT OVERWRITE TABLE $table SELECT * FROM $table_copy")
      //drop copy table
      dfsc.sql(s"DROP TABLE $table_copy")
      //show new table with deleted row
      dfsc.sql(s"SELECT * FROM $table").show(5)
    }
    }

    def scenario6(): Unit = {
      // Problem Scenario 6
      // Add future query
      
    }
      

/*
    SMALL_cappuccino, MED_cappuccino, LARGE_cappuccino, COLD_cappuccino, ICY_cappuccino, Triple_cappuccino, 
    Mild_cappuccino, Special_cappuccino, Double_cappuccino are the only beverages sold daily
    Sold both X&Y in the XYXYXY... pattern
    All others are sold either X or Y
*/







    













































/*
    println("xxxxxxxxxxxxxxxxxxxxx")
    val sc = new SparkContext("local[2]", "AppName")
    sc.setLogLevel("ERROR")
    sc.parallelize(Seq((1,3,4),(6,4,2),(6,4,9),(2,5,8),(4,1,3)))
    println(sc.parallelize(Seq((1,3,4),(6,4,2),(6,4,9),(2,5,8),(4,1,3))))
    println("yyyyyyyyyyyyyyyyyyyy")
    //val df3 = df5.toDF("col1", "col2", "col3")
    //val df5 = 
  
    .
    .
    .
    .
    .
    .
    .
    .
*/


/*
    val file = Source.fromFile("input/CountACut.txt")
    var coffee = ListBuffer[(String, BigDecimal)]()
    for (line<-file.getLines) {
      val a = line.split(",")
      coffee.append((a(0), a(1).toInt))
    }
    println(coffee)
    file.close
    var sparkCoffee = dfsc.sparkContext.parallelize(coffee.toList)
    println(sparkCoffee.getClass())
    var joined = sparkCoffee.groupByKey()
    joined.foreach(y=>{
      var y0 = y._1
      var y1 = y._2
      println(s"coffee is $y0 and y is $y1")
    })
    println(joined.getClass())
*/
/*
    val textFile = sc.textFile("input/SparkWordCountData")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey( + )
*/


/*
    /////////////////////////////////////////////////
    
    val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()
    

    // val data = DataStreamReader.csv("input/rtdata/countA/day1.csv")
    // val data1 = DataFrameReader.csv("input/rtdata/countA/day1.csv")

    // val data1 = spark.read.csv("input/rtdata/countA/day1.csv")

    
    val lines = sc.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)

    ////////////////////////////////////////////////

    val file = Source.fromFile("input/CountACut.txt")
    var coffee = ListBuffer[(String, BigDecimal)]()
    for (line<-file.getLines) {
      val a = line.split(",")
      coffee.append((a(0), a(1).toInt))
    }
    println(coffee)
    file.close
    var sparkCoffee = dfsc.sparkContext.parallelize(coffee.toList)
    println(sparkCoffee.getClass())
    var joined = sparkCoffee.groupByKey()
    joined.foreach(y=>{
      var y0 = y._1
      var y1 = y._2
      println(s"coffee is $y0 and y is $y1")
    })
    println(joined.getClass())

    ///////////////////////////////////////////////
*/



/*
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    //Logger.getRootLogger().setLevel(Level.OFF);
    
    val dfsc = SparkSession.builder()
    .appName("HiveTest5")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
    
    dfsc.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    dfsc.sql("create table if not exists newone2(id Int,name String) row format delimited fields terminated by ','");
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' OVERWRITE INTO TABLE newone2")
    dfsc.sql("SELECT * FROM newone2").show()
    dfsc.sql("SELECT * FROM newone2 WHERE id=23").show()
*/














  }
}