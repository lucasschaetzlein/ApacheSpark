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
   var tweetCount = 0;
   var overallTweetLength = 0;
      
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //connectToDB()
    
    def connectToDB(): Boolean = {
      //Herstellen einer Verbindung zur Datenbank
      val url = "jdbc:mysql://127.0.0.1:3306/twitter"
      val user = "admin"
      val pw = "admin"
  
      val con = DriverManager.getConnection(url, user, pw)
      if (con != null) {
        println("Verbindung zur Datenbank erfolgreich")
        return true;
      }
      return false;
    }
    
    //Initializieren von Spark
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //Twitter Credentials für die Verbindung zur Twitter API
    val consumerKey = "skEDCX0PnJ1iinYJHpKhkEPgl";
    val consumerSecret = "M3xqVHS2BEda9ULwT2SfjIe3Ls40hGYLw5trZ5JSEHMnCHDe0c";
    val accessToken = "803228483203764224-3Hg95OUm3IUZHhV0WyNVdmKOSIAiVK5";
    val accessTokenSecret = "aaws4sUJdXo6RqTYchx1e1iMgU6EG1SxQ6ELOBOXMPTDm";
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val filters = Array("Samsung")
    val stream = TwitterUtils.createStream(ssc, None)
    
    //Hier folgt Analyse der Tweets
    
    //val users = stream.map(status => status)
    //users.print()
    
    //numberOfTweets
    tweetLength();
    //mostPopularHashTags
    
    //Anzahl der Tweets in einem bestimmen Zeitraum zu einem bestimmten Thema
    def numberOfTweets() = {
      val tweets = stream.foreachRDD {
        rdd =>
          var number = rdd.count
          tweetCount += number.toInt
          println("Number of Tweets: "+tweetCount);
      }
    }
    
    //Ausgabe der durchschnittlichen Länge der Tweets zu einem bestimmten Zeitraum
    def tweetLength() = {  
      val tweets = stream.foreachRDD {
        rdd =>
           for (tuple <- rdd) {
             var tweetLength = tuple.getText.length
             overallTweetLength += tweetLength;
             tweetCount += 1;
             //println(tuple.getUser.getName+" | "+tweetLength);
             println("Durchschnittliche Länge der Tweets: "+overallTweetLength/tweetCount+" <== Gesamtlänge: "+overallTweetLength+" | Anzahl der Tweets: "+tweetCount);
             //println("-------------------------------------------------");
           }
      }
    }
    
    //Beliebtesten Hashtags über einen Zeitraum
    def mostPopularHashTags() = {
      val hashTags = stream.flatMap(status => status.getHashtagEntities)
      // Convert hashtag to (hashtag, 1) pair for future reduction.
      val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
      // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
      // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).
      val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(10))
      
      // topCounts10 will provide a new RDD for every window. Calling transform()
      // on each of these RDDs gives us a per-window transformation. We use
      // this transformation to sort each RDD by the hashtag counts. The FALSE
      // flag tells the sortBy() function to sort in descending order.
      val sortedTopCounts10 = topCounts10.transform(rdd => 
      rdd.sortBy(hashtagPair => hashtagPair._2, false))
  
      // Print popular hashtags.
      sortedTopCounts10.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
      })
      
    }
    
    //Analyse der Tweets auf Freundlichkeit (Wörter zählen)  
    
    
/*
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
*/
    ssc.start()
    ssc.awaitTerminationOrTimeout(40000)
    //ssc.stop()
    
  }
}