package org.gistic.taghreed.dataanalysis.datasource;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.gistic.taghreed.dataanalysis.twitter.Tweet;

/**
 *
 * @author Amgad Madkour
 */
public class DataSourceWrapper {

    public HashMap<Long, String> map;

    public DataSourceWrapper() {
        map = new HashMap<Long, String>();
    }

    public ArrayList<Tweet> getFromMongo() {

        ArrayList<Tweet> tweets;
        Tweet tweet;

        tweets = new ArrayList<Tweet>();

        try {

            MongoClient mongoClient = new MongoClient("localhost");
            DB db = mongoClient.getDB("taghreed");
            DBCollection coll = db.getCollection("tweets");
            DBObject dbo;

            DBCursor cursor = coll.find();

            while (cursor.hasNext()) {
                dbo = cursor.next();
                tweet = new Tweet();
                tweet.setTweetId(((Number) dbo.get("tweet_id")).longValue());
                tweet.setScreenName((String) dbo.get("screen_name"));
                tweet.setCreatedAt((String) dbo.get("created_at"));
                tweet.setFollowersCount(((Number) dbo.get("followers_count")).intValue());
                tweet.setLang((String) dbo.get("lang"));
                tweet.setLatitude(((Number) dbo.get("lat")).doubleValue());
                tweet.setLongitude(((Number) dbo.get("long")).doubleValue());
                tweet.setUserId(((Number)dbo.get("user_id")).longValue());
                tweet.setText((String) dbo.get("text"));
                tweets.add(tweet);
            }

        } catch (UnknownHostException ex) {
            Logger.getLogger(DataSourceWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return tweets;
    }

}
