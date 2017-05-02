import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Properties
import java.sql.DriverManager
import java.sql.PreparedStatement
//import org.apache.spark.sql.SQLContext.implicits._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object Twitter {
  val url = "jdbc:db2://dashdb-entry-yp-dal09-10.services.dal.bluemix.net:50000/BLUDB"
   val user = "dash7107"
   val pw = "1D4Sjrcw4S5o"

   val con = DriverManager.getConnection(url, user, pw)
    //if(con != null){
    //    println("Verbindung erfolgreich")
    //}

   val SQL="insert into twitter values (?)"
   val ps = con.prepareStatement(SQL)
    
   val sc = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[16]")
   val ssc = new StreamingContext(sc, Seconds(2))

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
    val users = stream.map(status => status.getUser.getName)
    users.print()
    
    users.foreachRDD(rdd => {
      println("\nNumber of users in last 60 seconds (%s total):".format(rdd.count()))
      rdd.foreach{
        case ("") => println("ende")
        val singleUser = format(user)
        ps.setString(1, user)
        ps.execute()
        println("Inserted Twitter User into DB: ")
      }
    }


   ssc.start()
   ssc.awaitTermination()
}