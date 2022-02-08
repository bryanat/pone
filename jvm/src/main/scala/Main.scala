import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import scala.io.StdIn.readLine

import org.apache.spark._
import org.apache.spark.streaming._



object HiveTest5 {

  def main(args: Array[String]): Unit = {

    println("init72")










/*
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //System.setProperty("hive.root.logger", "console")
    //System.setProperty("log4j.rootCategory", "WARN, console")

    
    //Logger.getRootLogger().setLevel(Level.OFF);
    
    val spark1 = SparkSession.builder()
    .appName("HiveTest5")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

    spark1.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark1.sql("create table if not exists newone2(id Int,name String) row format delimited fields terminated by ','");
    spark1.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' OVERWRITE INTO TABLE newone2")
    spark1.sql("SELECT * FROM newone2").show()
    spark1.sql("SELECT * FROM newone2 WHERE id=23").show()
*/


  }
}
