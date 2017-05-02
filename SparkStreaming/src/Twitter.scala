import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Properties
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.sql.{ Connection, DriverManager, ResultSet }

object Twitter {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Herstellen einer Verbindung zur Datenbank
    val url = "jdbc:mysql://127.0.0.1:3306/twitter"
    val user = "admin"
    val pw = "admin"

    val con = DriverManager.getConnection(url, user, pw)
    if (con != null) {
      println("Verbindung zur Datenbank erfolgreich")
    }

    //Initializieren von Spark
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[16]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    //Twitter Credentials für die Verbindung zur Twitter API
    val consumerKey = "skEDCX0PnJ1iinYJHpKhkEPgl";
    val consumerSecret = "M3xqVHS2BEda9ULwT2SfjIe3Ls40hGYLw5trZ5JSEHMnCHDe0c";
    val accessToken = "803228483203764224-3Hg95OUm3IUZHhV0WyNVdmKOSIAiVK5";
    val accessTokenSecret = "aaws4sUJdXo6RqTYchx1e1iMgU6EG1SxQ6ELOBOXMPTDm";
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val filters = Array("UnboxYourPhone", "Samsung")
    val stream = TwitterUtils.createStream(ssc, None, filters)
    //val stream = scc.twitterStream()
    val users = stream.map(status => status.getUser.getName)
    users.print()

    //Speichern der Tweet Autoren in der Datenbank
    val tweets = stream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          it =>
            val conn = DriverManager.getConnection(url, user, pw)
            val del = conn.prepareStatement("INSERT INTO twitter (user) VALUES (?)")
            for (tuple <- it) {
              del.setString(1, tuple.getUser.getName)
              del.executeUpdate
            }
            conn.close()
        }
    }

    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()
  }
}