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


object Twitter {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(597); 
  val url = "jdbc:db2://dashdb-entry-yp-dal09-10.services.dal.bluemix.net:50000/BLUDB";System.out.println("""url  : String = """ + $show(url ));$skip(25); 
   val user = "dash7107";System.out.println("""user  : String = """ + $show(user ));$skip(27); 
   val pw = "1D4Sjrcw4S5o";System.out.println("""pw  : String = """ + $show(pw ));$skip(57); 

   val con = DriverManager.getConnection(url, user, pw);System.out.println("""con  : java.sql.Connection = """ + $show(con ));$skip(120); 
    //if(con != null){
    //    println("Verbindung erfolgreich")
    //}

   val SQL="insert into twitter values (?)";System.out.println("""SQL  : String = """ + $show(SQL ));$skip(38); 
   val ps = con.prepareStatement(SQL);System.out.println("""ps  : java.sql.PreparedStatement = """ + $show(ps ));$skip(89); 
    
   val sc = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[16]");System.out.println("""sc  : org.apache.spark.SparkConf = """ + $show(sc ));$skip(50); 
   val ssc = new StreamingContext(sc, Seconds(2));System.out.println("""ssc  : org.apache.spark.streaming.StreamingContext = """ + $show(ssc ));$skip(51); 

   val consumerKey = "skEDCX0PnJ1iinYJHpKhkEPgl";System.out.println("""consumerKey  : String = """ + $show(consumerKey ));$skip(78); ;
   val consumerSecret = "M3xqVHS2BEda9ULwT2SfjIe3Ls40hGYLw5trZ5JSEHMnCHDe0c";System.out.println("""consumerSecret  : String = """ + $show(consumerSecret ));$skip(75); ;
   val accessToken = "803228483203764224-3Hg95OUm3IUZHhV0WyNVdmKOSIAiVK5";System.out.println("""accessToken  : String = """ + $show(accessToken ));$skip(76); ;
   val accessTokenSecret = "aaws4sUJdXo6RqTYchx1e1iMgU6EG1SxQ6ELOBOXMPTDm";System.out.println("""accessTokenSecret  : String = """ + $show(accessTokenSecret ));$skip(66); val res$0 = ;
   System.setProperty("twitter4j.oauth.consumerKey", consumerKey);System.out.println("""res0: String = """ + $show(res$0));$skip(72); val res$1 = 
   System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);System.out.println("""res1: String = """ + $show(res$1));$skip(66); val res$2 = 
   System.setProperty("twitter4j.oauth.accessToken", accessToken);System.out.println("""res2: String = """ + $show(res$2));$skip(78); val res$3 = 
   System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);System.out.println("""res3: String = """ + $show(res$3));$skip(63); 

   
   

    val filters = Array("UnboxYourPhone", "Samsung");System.out.println("""filters  : Array[String] = """ + $show(filters ));$skip(63); 
    val stream = TwitterUtils.createStream(ssc, None, filters);System.out.println("""stream  : org.apache.spark.streaming.dstream.ReceiverInputDStream[twitter4j.Status] = """ + $show(stream ));$skip(61); 
    val users = stream.map(status => status.getUser.getName);System.out.println("""users  : org.apache.spark.streaming.dstream.DStream[String] = """ + $show(users ));$skip(18); 
    users.print();$skip(374); val res$4 = 
    
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
   ssc.awaitTermination();System.out.println("""res4: <error> = """ + $show(res$4))}
}
