/* TwitterClient Application
   * Uses Twitter4j library (java)
   * Uses Twitter API 1.1
   * Elisha - Simple Developer
   */


import java.io.IOException;


import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
 
public class TwitterStreaming {
 
     public static void main(String[] args) throws IOException, TwitterException {
 
        //Your Twitter App's Consumer Key
        String consumerKey = "skEDCX0PnJ1iinYJHpKhkEPgl";
 
        //Your Twitter App's Consumer Secret
        String consumerSecret = "M3xqVHS2BEda9ULwT2SfjIe3Ls40hGYLw5trZ5JSEHMnCHDe0c";
 
        //Your Twitter Access Token
        String accessToken = "803228483203764224-3Hg95OUm3IUZHhV0WyNVdmKOSIAiVK5";
 
        //Your Twitter Access Token Secret
        String accessTokenSecret = "aaws4sUJdXo6RqTYchx1e1iMgU6EG1SxQ6ELOBOXMPTDm";
 
        //Instantiate a re-usable and thread-safe factory
        TwitterFactory twitterFactory = new TwitterFactory();
 
        //Instantiate a new Twitter instance
        Twitter twitter = twitterFactory.getInstance();
 
        //setup OAuth Consumer Credentials
        twitter.setOAuthConsumer(consumerKey, consumerSecret);
 
        //setup OAuth Access Token
        twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
 
        /*
        //Instantiate and initialize a new twitter status update
        StatusUpdate statusUpdate = new StatusUpdate(
                //your tweet or status message
                "Hello World - Java + Twitter API");
        
        //attach any media, if you want to
        statusUpdate.setMedia(
                //title of media
                "http://simpledeveloper.com"
                , new URL("https://si0.twimg.com/profile_images/1733613899/Published_Copy_Book.jpg").openStream());
        
        //tweet or update status
        Status status = twitter.updateStatus(statusUpdate);
 
        //response from twitter server
        System.out.println("status.toString() = " + status.toString());
        System.out.println("status.getInReplyToScreenName() = " + status.getInReplyToScreenName());
        System.out.println("status.getSource() = " + status.getSource());
        System.out.println("status.getText() = " + status.getText());
        
        System.out.println("status.getURLEntities() = " + Arrays.toString(status.getURLEntities()));
        System.out.println("status.getUserMentionEntities() = " + Arrays.toString(status.getUserMentionEntities()));
        */
         
        
    }
 
  }