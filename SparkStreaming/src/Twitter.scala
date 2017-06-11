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
    
    //Initializieren von Spark
    val sparkConf = new SparkConf().setAppName("TwitterStreaming").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    //Twitter Credentials für die Verbindung zur Twitter API
    val consumerKey = "skEDCX0PnJ1iinYJHpKhkEPgl";
    val consumerSecret = "M3xqVHS2BEda9ULwT2SfjIe3Ls40hGYLw5trZ5JSEHMnCHDe0c";
    val accessToken = "803228483203764224-3Hg95OUm3IUZHhV0WyNVdmKOSIAiVK5";
    val accessTokenSecret = "aaws4sUJdXo6RqTYchx1e1iMgU6EG1SxQ6ELOBOXMPTDm";
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Filter für die Tweets: Folgende Wörter müssen enthalten sein
    val filters = Array("love")
    
    //Aufruf der Analyse Funktion
    
    //numberOfTweets(ssc, filters)
    //tweetLength(ssc, filters)
    //mostPopularHashTags(ssc, 20, 5)
    getFollowerOfAuhors(ssc, filters)
    //saveTweetAuthorsToDB(ssc, filters)
    
    ssc.start()
    //ssc.awaitTermination()
    ssc.awaitTerminationOrTimeout(20000)
    ssc.stop()
    
   }
  
    //Anzahl der Tweets in einem bestimmen Zeitraum zu einem bestimmten Thema
    def numberOfTweets(ssc: StreamingContext, filters: Array[String]) = {
      val stream = TwitterUtils.createStream(ssc, None, filters)
      val startTime = System.currentTimeMillis
      
      val tweets = stream.foreachRDD {
        (rdd, time) =>
          var number = rdd.count
          tweetCount += number.toInt
          println("Number of Tweets of the last "+(time.milliseconds-startTime)/1000+" seconds: "+tweetCount);
      }
    }
    
    //Ausgabe der durchschnittlichen Länge der Tweets zu einem bestimmten Zeitraum
    def tweetLength(ssc: StreamingContext, filters: Array[String]) = {  
      var averageTweetLength = 0
      val stream = TwitterUtils.createStream(ssc, None, filters)
      val startTime = System.currentTimeMillis
      
      val tweets = stream.foreachRDD {
        (rdd, time) =>
           for (tuple <- rdd) {
             var tweetLength = tuple.getText.length
             overallTweetLength += tweetLength;
             tweetCount += 1;
             //println(tuple.getUser.getName+" | "+tweetLength);
             //println("Durchschnittliche Länge der Tweets: "+overallTweetLength/tweetCount+" <== Gesamtlänge: "+overallTweetLength+" | Anzahl der Tweets: "+tweetCount);
             //println("-------------------------------------------------");
           }
           if(tweetCount != 0){
             averageTweetLength = overallTweetLength/tweetCount
           }
           println("Durchschnittliche Länge der Tweets nach "+(time.milliseconds-startTime)/1000+" Sekunden: "+averageTweetLength+" Zeichen | Gesamtlänge aller Tweets: "+overallTweetLength+" | Anzahl der Tweets: "+tweetCount);
      }
    }
    
    //Beliebtesten Hashtags über einen Zeitraum
    def mostPopularHashTags(ssc: StreamingContext, seconds: Int, size: Int) = {
      val stream = TwitterUtils.createStream(ssc, None)
      val hashTags = stream.flatMap(status => status.getHashtagEntities)
      // Convert hashtag to (hashtag, 1) pair for future reduction.
      val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
      
      // Use reduceByKeyAndWindow to reduce our hashtag pairs by summing their
      // counts over the last 10 seconds of batch intervals (in this case, 2 RDDs).
      val topCounts10 = hashTagPairs.reduceByKeyAndWindow((l, r) => {l + r}, Seconds(seconds))
      
      // topCounts10 will provide a new RDD for every window. Calling transform()
      // on each of these RDDs gives us a per-window transformation. We use
      // this transformation to sort each RDD by the hashtag counts. The FALSE
      // flag tells the sortBy() function to sort in descending order.
      val sortedTopCounts10 = topCounts10.transform(rdd => 
      rdd.sortBy(hashtagPair => hashtagPair._2, false))
  
      // Print popular hashtags.
      sortedTopCounts10.foreachRDD((rdd,time) => {
        val topList = rdd.take(size)
        println("\n"+size+" most popular hashtags in last "+seconds+" seconds (total tweets in last 20 seconds: %s):".format(rdd.count()))
        topList.foreach{case (tag, count) => println("%s (%d tweets)".format(tag, count))}
      })

      
    }
    
    //Analyse der Tweets auf Freundlichkeit (Wörter zählen)  
    
    //Anzahl der Follower jedes Tweetautoren
    def getFollowerOfAuhors(ssc: StreamingContext, filters: Array[String]) = {
      val stream = TwitterUtils.createStream(ssc, None, filters)
      val tweets = stream.foreachRDD {
        (rdd, time) =>
           for (tuple <- rdd) {
             println("User: "+tuple.getUser.getName+" | Number of Follower: "+tuple.getUser.getFollowersCount);
             //println("-------------------------------------------------");
           }
      }
    }
    
    def saveTweetAuthorsToDB(ssc: StreamingContext, filters: Array[String]) = {
       //Herstellen einer Verbindung zur Datenbank
      val url = "jdbc:mysql://127.0.0.1:3306/twitter"
      val user = "admin"
      val pw = "admin"
  
      val con = DriverManager.getConnection(url, user, pw)
      if (con != null) {
        println("Verbindung zur Datenbank erfolgreich")
      }else{
        println("Verbindung zur Datenbank nicht erfolgreich")
      }
      
      val stream = TwitterUtils.createStream(ssc, None, filters)
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
     }
       
}