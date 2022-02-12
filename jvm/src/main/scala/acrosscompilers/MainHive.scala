package acrosscompilers

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.ml._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer



object MainHive {
  // def main(args: Array[String]): Unit = {
  def Xmain(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val dfsc = SparkSession.builder().appName("HiveApp").config("spark.master", "local").enableHiveSupport().getOrCreate()
    dfsc.sparkContext.setLogLevel("ERROR")

    // BranchABC table contains data from all three Bev_Branch[A,B,C].txt files
    dfsc.sql("CREATE TABLE IF NOT EXISTS BranchABC(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BranchABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BranchABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BranchABC")
    // CountABC table contains data from all three Bev_Consciount[A,B,C].txt files
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountABC(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' OVERWRITE INTO TABLE CountABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE CountABC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE CountABC")
    // BranchA table
    dfsc.sql("CREATE TABLE IF NOT EXISTS BranchA(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BranchA")
    // BranchB table
    dfsc.sql("CREATE TABLE IF NOT EXISTS BranchB(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' OVERWRITE INTO TABLE BranchB")
    // BranchC table
    dfsc.sql("CREATE TABLE IF NOT EXISTS BranchC(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' OVERWRITE INTO TABLE BranchC")
    // CountA table
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountA(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' OVERWRITE INTO TABLE CountA")
    // CountB table
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountB(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' OVERWRITE INTO TABLE CountB")
    // CountC table
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountC(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' OVERWRITE INTO TABLE CountC")
    // CountAC table
    dfsc.sql("CREATE TABLE IF NOT EXISTS CountAC(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' OVERWRITE INTO TABLE CountAC")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE CountAC")
    // DummyCountB table
    dfsc.sql("CREATE TABLE IF NOT EXISTS DummyCountB(beverage STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' OVERWRITE INTO TABLE DummyCountB")
  

    def initializeMenu() = {
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
          |6. Scenario 6 - Future Query Spark Streaming realtime consumer count transactions
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
            case 0 => System.exit(0)
            case 1 => {scenario1(); menu()}     
            case 2 => {scenario2(); menu()}
            case 3 => {scenario3(); menu()}
            case 4 => {scenario4(); menu()}
            case 5 => {scenario5(); menu()}
            case 6 => {dfsc.stop(); }  //MainStreaming.init()
            case _ => {println("Invalid number, pick a number from the menu."); menu()} // catch all for non numbers 0-7, recursive menu() call to loop 
          }
        }
      }
      menu() 
    }
    initializeMenu()


    // Problem Scenario 1 
    def scenario1(): Unit = {
      // What is the total number of consumers for Branch1?
      // Branch1 only exists on BranchA
      dfsc.sql("SELECT SUM(CountA.count) AS Total_Consumers_Branch1 FROM CountA JOIN BranchABC AS b ON CountA.beverage = b.beverage WHERE b.branch='Branch1'").show(20)
      // What is the number of consumers for the Branch2?
      dfsc.sql("SELECT SUM(CountAC.count) AS Total_Consumers_Branch2 FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2'").show(20)
    // Method used was Type 1: a single table with sub queries.
    // over the method Type 2: multiple tables which consume more memory in Spark.
    // the main downside to Type 1 is query performance, but if you wanted to optimize queries you could partition the table

    }

    // Problem Scenario 2 
    def scenario2(): Unit = {
      // What is the most consumed beverage on Branch1 max() //special_cappuccino = 108163 
      dfsc.sql("SELECT BranchABC.beverage, sum(CountABC.count) FROM BranchABC JOIN CountABC ON BranchABC.beverage = CountABC.beverage WHERE branch='Branch1' GROUP BY BranchABC.beverage ORDER BY sum(CountABC.count) DESC LIMIT 1").show()
      
      // What is the least consumed beverage on Branch2 min() //Cold_MOCHA = 47524
      dfsc.sql("SELECT BranchABC.beverage, sum(CountABC.count) FROM BranchABC JOIN CountABC ON BranchABC.beverage = CountABC.beverage WHERE branch='Branch2' GROUP BY BranchABC.beverage ORDER BY sum(CountABC.count) ASC LIMIT 1").show()

      // dfsc.sql("CREATE TABLE IF NOT EXISTS Branch1(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
      // dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Branch1")
      // dfsc.sql("SELECT * FROM Branch1 WHERE branch='Branch1'")
      // dfsc.sql("SELECT Branch1.beverage, first(count) FROM Branch1 INNER JOIN CountABC AS ctable ON ctable.beverage = Branch1.beverage GROUP BY Branch1.beverage ORDER BY count DESC LIMIT 1").show()
      // dfsc.sql("CREATE TABLE IF NOT EXISTS Branch2(beverage STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
      // dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE Branch2")
      // dfsc.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Branch2")
      // dfsc.sql("SELECT * FROM Branch1 WHERE branch='Branch1'").show()
      //dfsc.sql("CREATE TABLE IF NOT EXISTS Branch1(beverage STRING, branch STRING) AS (SELECT * FROM BranchABC WHERE branch='Branch1')")
      // dfsc.sql("SELECT CountA.beverage, SUM(count) AS Most_Consumed FROM CountA JOIN BranchABC AS b ON CountA.beverage = b.beverage WHERE b.branch='Branch1' ORDER BY CountA.count DESC LIMIT 1")
  // dfsc.sql("CREATE VIEW IF NOT EXISTS myview AS SELECT * FROM BranchABC")
      // dfsc.sql("SELECT Branch1.beverage, count FROM CountABC JOIN Branch1 AS b ON CountABC.beverage = b.beverage ORDER BY count LIMIT 1").show()
  //dfsc.sql("SELECT Branch1.beverage, count FROM CountABC JOIN Branch1 AS b ON CountABC.beverage = b.beverage").show()
    //dfsc.sql("SELECT CountA.beverage, SUM(count) FROM CountA JOIN BranchABC AS b ON CountA.beverage = b.beverage WHERE b.branch='Branch1' GROUP BY CountA.count ORDER BY CountA.count DESC LIMIT 1").show
      // dfsc.sql("SELECT CountAC.beverage, SUM(count) AS Least_Consumed FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2' ORDER BY CountAC.count LIMIT 1")
  //dfsc.sql("SELECT CountAC.beverage, count FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2' ORDER BY CountAC.count LIMIT 1").show
      // What is the Average consumed beverage of Branch2 interpretted as Mean avg()
      // dfsc.sql("SELECT CountAC.beverage, AVG(CountAC.count) AS Average_Consumed FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2' GROUP BY CountAC.count ORDER BY CountAC.count ASC")
      // dfsc.sql("SELECT CountAC.beverage, AVG(count) AS Average_Consumed FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2'")
  //dfsc.sql("SELECT CountAC.beverage, AVG(count) FROM CountAC JOIN BranchABC AS b ON CountAC.beverage = b.beverage WHERE b.branch='Branch2'").show
    //!!! QUESTION #2 Average can be interpretted in 2 ways: mean and median: mean of each beverage, median of overall table
      // What is the Average consumed beverage of Branch2 interpretted as Median: 
    //dfsc.sql("CREATE TABLE IF NOT EXISTS Beverage_Sum AS SELECT CountA.beverage, sum(CountA.count) AS Total_Beverage FROM BranchABC JOIN CountA ON BranchABC.beverage= CountA.beverage group by CountA.beverage").show()
      // Add Row column
    //dfsc.sql("CREATE TABLE IF NOT EXISTS NEW SELECT *, ROW_NUMBER() OVER (ORDER BY Total_Beverage) AS row FROM Beverage_Sum")
      // Median
      //dfsc.sql("SELECT beverage as average_consumed_beverage, count(beverage) as countofall from NEW where row=round((countofall/2), 0)").show()
    //dfsc.sql("select CountA.beverage, sum(CountA.count) as Total_Beverage from BranchA Join CountA on BranchA.beverage=CountA.beverage where BranchA.branch='Branch1' group by CountA.beverage").show()
      // dfsc.sql("select CountA.beverages, sum(CountA.count) as Total_Beverage from BranchA Join CountA on BranchA.beverage=CountA.beverage where BranchA.branch='Branch1' group by CountA.beverage order by beverages_sum desc ")
      /*
     

        //prints out two tables, first showing branch intersections
        //second is the top 3 of most consumed
        
        dfsc.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')")
        dfsc.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        dfsc.sql("SELECT CountA.beverages, sum(CountA.count) as Total_Beverage FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages order by beverage_sum desc").show(3)
        
        // //#2
        // ("select CountA.beverages, sum(CountA.count) as Total_Beverage from BranchesA Join CountA on BranchesA.beverages=CountA.beverages where BranchesA.branches='Branch1' group by CountA.beverages order by beverages_sum desc ")
      
          // dfsc.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')").show(60, false)
        // dfsc.sql("SELECT sum(CountA.count) as Total_Consumers FROM  BranchesA JOIN CountA ON BranchesA.beverages= CountA.beverages WHERE BranchesA.branches = 'Branch1'").show()

      */
    }

    def scenario3(): Unit = {
      // Problem Scenario 3
      // What are the beverages available on Branch10, Branch8, and Branch1?
      //dfsc.sql("CREATE TABLE IF NOT EXISTS Branch81 AS SELECT beverage, branch FROM ")
      println("Beverages available on Branch1 or Branch8 (no Branch10)")
      dfsc.sql("SELECT DISTINCT beverage, branch FROM BranchABC WHERE branch='Branch8' OR branch='Branch1' or branch='Branch10' ORDER BY branch ASC").show(150)
      // what are the comman beverages available in Branch4,Branch7?
      println("Most common beverages available on Branch4 or Branch7")
  // dfsc.sql("SELECT BranchABC.beverage, count FROM BranchABC JOIN CountABC ON BranchABC.beverage = CountABC.beverage WHERE BranchABC.branch='Branch4' OR BranchABC.branch='Branch7' ORDER BY count DESC ").show(20)
      dfsc.sql("SELECT first(BranchABC.beverage), count FROM BranchABC JOIN CountABC ON BranchABC.beverage = CountABC.beverage WHERE BranchABC.branch='Branch4' OR BranchABC.branch='Branch7' GROUP BY count ORDER BY count DESC ").show(20)
       // QUESTION #3 MAYBE USE collect_set(branch) IN HIVE QUERY

    }
      
    // Problem Scenario 4
    def scenario4(): Unit = {
      // create a partition,View for the scenario3.
    //dfsc.sql("DROP TABLE Part_Branch")
      println("Partitioning table...")
      dfsc.sql("CREATE TABLE IF NOT EXISTS Part_Branch(beverage STRING) PARTITIONED BY (branch STRING)")
      dfsc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      dfsc.sql("INSERT OVERWRITE TABLE Part_Branch PARTITION(branch) SELECT beverage, branch from BranchABC")
      dfsc.sql("SELECT * FROM Part_Branch").show(150)
      println("Partitioned table successfully completed.")
      // dfsc.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches = '$b2'")
      // dfsc.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
      // dfsc.sql(s"SELECT DISTINCT beverages FROM (SELECT beverages, collect_set(branches) as b FROM Partitioned group by beverages) WHERE ARRAY_CONTAINS(b, '$b1') AND ARRAY_CONTAINS(b, '$b2')").show()
      // dynamic partition: dfsc.sql("CREATE TABLE IF NOT EXISTS Partitioned(beverages STRING) COMMENT 'A PARTITIONED BRANCH TABLE' PARTITIONED BY (branches STRING)")
      // dfsc.sql("INSERT OVERWRITE TABLE Partitioned PARTITION(branches) SELECT beverages,branches from Branches")
      // dfsc.sql("SELECT * FROM Partitioned")
      // ssql.sql("SHOW PARTITIONS BranchTablePartitioned").show()
    } 
    /*
    println("Partitioning table...")
      dfsc.sql("CREATE TABLE IF NOT EXISTS Part_Branch(beverage STRING) PARTITIONED BY (branch STRING)")
      dfsc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      dfsc.sql("INSERT OVERWRITE TABLE Part_Branch PARTITION(branch) SELECT beverage, branch from BranchABC")
      dfsc.sql("SELECT * FROM Part_Branch").show(150)
      println("Partitioned table successfully completed.")
    */
    
    // Problem Scenario 5
    def scenario5(): Unit = {
      // Alter the table properties to add "note","comment"
      // Problem Scenario 5: Alter the table properties to add "note","comment"
      // notes = Comment
      // SHOW TBLPROPERTIES <tablename>
      
      var inputTableName = readLine("Type the table you would like to add the note to: ")
      var inputNote = readLine(s"Type the note you would like to add to $inputTableName: ")
      dfsc.sql(s"ALTER TABLE $inputTableName SET TBLPROPERTIES ('notes' = '$inputNote')")
      dfsc.sql(s"SHOW TBLPROPERTIES $inputTableName").show()
      //dfsc.sql("DESCRIBE FORMATTED table_name").show()

      var nullinput = readLine("Press any key to continue to delete a record from CountB")
      // Remove a row from the any Senario.
      def deleteRowNoParams(): Unit = {
        println("Table before Delete:")
        dfsc.sql("SELECT * FROM DummyCountB").show(5)
        //create copy table
        dfsc.sql("CREATE TABLE IF NOT EXISTS DummyCountB_copy LIKE DummyCountB")
        //load data into copy table except deleted item
        dfsc.sql("INSERT INTO DummyCountB_copy SELECT * FROM DummyCountB WHERE DummyCountB.beverage NOT IN (SELECT DummyCountB.beverage FROM DummyCountB WHERE DummyCountB.beverage='Special_Lite')")
        //overwrite copy table to original table
        dfsc.sql("INSERT OVERWRITE TABLE DummyCountB SELECT * FROM DummyCountB_copy")
        //drop copy table
        dfsc.sql("DROP TABLE DummyCountB_copy")
        //show new table with deleted row
        println("Table after Delete:")
        dfsc.sql("SELECT * FROM DummyCountB").show(5)
      }
      deleteRowNoParams()

      /*
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
      */
    }

  // A method that returns All 54 Beverages in Map, will be used in Spark Streaming and Scala-js
  // Broadcast the The54Beverages variable instead of starting an entire SparkSession inside other objects just to run a single Hive query ("SELECT DISTINCT beverage FROM BranchABC"), working similarily to variable hoisting in this case
  // In Spark Streaming each of the 4 threads will contain a cached copy of the 
  def getBroadcastBeverageMap(): Broadcast[Map[String,Int]] = {

    val dfsc = SparkSession.builder().appName("HiveTest5").config("spark.master", "local").enableHiveSupport().getOrCreate()
    dfsc.sparkContext.setLogLevel("ERROR")

    // First generate a list of all 54 beverages 
    var dfres = dfsc.sql("SELECT DISTINCT beverage FROM BranchABC").collect()
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

    //BROADCAST beverageMap WITH THE OTHER OBJECTS (could just abstract through an object property) 
    val broadcastBeverageMap: Broadcast[Map[String,Int]] = dfsc.sparkContext.broadcast(beverageMap)
    broadcastBeverageMap
  }
  
    
    def scenario6(): Unit = {
      // Problem Scenario 6
      // Add future query

      println(getBroadcastBeverageMap().value)

    }
      

/*
    SMALL_cappuccino, MED_cappuccino, LARGE_cappuccino, COLD_cappuccino, ICY_cappuccino, Triple_cappuccino, 
    Mild_cappuccino, Special_cappuccino, Double_cappuccino are the only beverages sold daily
    Sold both X&Y in the XYXYXY... pattern
    All others are sold either X or Y
*/

  }



}

