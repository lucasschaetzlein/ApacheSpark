import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

object FlightData {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FlightData").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val spark = new SQLContext(sc)
    
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("../GraphX/ressource/2008.csv")
    df.printSchema()
  }
}