import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
//import org.apache.spark.ml._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer



object MainHive {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val dfsc = SparkSession.builder().appName("HiveTest5").config("spark.master", "local").enableHiveSupport().getOrCreate()
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
            case 0 => System.exit(0)
            case 1 => {scenario1(); menu()}     
            case 2 => {scenario2(); menu()}
            case 3 => {scenario3(); menu()}
            case 4 => {scenario4(); menu()}
            case 5 => {scenario5(); menu()}
            case 6 => {scenario6(); menu()}
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

      // First generate a list of all 54 beverages
      // 
      var dfres = dfsc.sql("SELECT DISTINCT beverage FROM BranchABC").collect()
      // Create an empty array, that will end up holding the name of each beverage 
      var beverageArray: ArrayBuffer[String] = ArrayBuffer[String]()
      // Generate an array containing each beverage name 
      dfres.foreach( row => {
        // Add each row to the beverage array after converting it to a string 
        beverageArray += row.get(0).toString()
      })
      beverageArray.length //54 Beverages

      // A little trick to turn an Array into comma separate strings
      var The54MinusEndQuotes = beverageArray.mkString("\",\"")

      var The54Seq = beverageArray.toSeq

      println("ibdsjfsdjfbk")
      // The 54 Beverages
      val The54Beverages = Seq(The54MinusEndQuotes)
      The54Seq.foreach(beverage => {
        println(beverage)
      })
      println(The54Beverages)
      println("ibdsjfsdjfbk")

      println("ahhhhhhh")
      beverageArray.foreach(beverage => {
        println(beverage)
      })
      println("ahhhhhhhHHH")

      var bevarageMap = beverageArray.map(beverage => (beverage, 0)).toMap
      bevarageMap
      println("OOOOOOOOxxxxOOOOOOOOOOO")
      println(bevarageMap)
      println("HJBAKHDBUSHAUY")
      println(bevarageMap.getClass())



    }
      

/*
    SMALL_cappuccino, MED_cappuccino, LARGE_cappuccino, COLD_cappuccino, ICY_cappuccino, Triple_cappuccino, 
    Mild_cappuccino, Special_cappuccino, Double_cappuccino are the only beverages sold daily
    Sold both X&Y in the XYXYXY... pattern
    All others are sold either X or Y
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


  }
}
