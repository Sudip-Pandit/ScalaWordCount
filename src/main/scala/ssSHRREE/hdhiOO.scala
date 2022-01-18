package ssSHRREE

import org.apache.spark.sql.SparkSession
object hdhiOO {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .master(master="local")
      .appName("firstPP")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("OFF")
    val input_path = "file:///C:/Users/Sudip/Desktop/pyspark-project/2010-summary1.txt"
    val output_path =" "
    val rdd1 = sc.textFile(input_path).flatMap(line => line.split(" ")).map(word => (word,1))
      .reduceByKey(_+_)
    rdd1.foreach(println)

  }

}
