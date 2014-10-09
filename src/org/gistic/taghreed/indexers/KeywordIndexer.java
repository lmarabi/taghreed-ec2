package org.gistic.taghreed.indexers;

import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.crawler.Crawler;
import org.gistic.taghreed.dataanalysis.search.Indexer;
import org.gistic.taghreed.dataanalysis.twitter.Tweet;
import org.gistic.taghreed.json.JSONArray;
import org.gistic.taghreed.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by saifalharthi on 5/18/14.
 */
public class KeywordIndexer {

    boolean indexClosed = false;
    IndexWriterConfig icw = new IndexWriterConfig(Version.LUCENE_45 , new ArabicAnalyzer(Version.LUCENE_45));
    IndexWriter writer = null;

    {
        try {
            writer = new IndexWriter(Crawler.ramDir , icw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseAndIndexNormalized(String tweetMessage) throws ParseException, java.text.ParseException {


        List<String> hashtags = new ArrayList<String>();
        // Parse Tweets
        JSONObject twitterPost = new JSONObject(tweetMessage);
        JSONObject user = twitterPost.getJSONObject("user");
        String created_at = fixDate(twitterPost.getString("created_at"));
        long tweet_id = Long.parseLong(twitterPost.getString("id_str"));
        long user_id = Long.parseLong(user.getString("id_str"));
        String screen_name = user.getString("screen_name");
        String tweetText = twitterPost.getString("text");
        String fixedTweetText = tweetText.replace('\n', ' ');
        String anotherFixedTweetText = fixedTweetText.replace(",",".");
        String yetAnotherFixedTweeet = anotherFixedTweetText.replaceAll("[\\t\\n\\r]"," ");
        int follower_count =user.getInt("followers_count");
        String language = user.getString("lang");
        String source = twitterPost.getString("source");


        double[] coordinates = new double[2];

        try {
            JSONObject geo = twitterPost.getJSONObject("geo");
            JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
            coordinates[0] = coordsJSON.getDouble(0);
            coordinates[1] = coordsJSON.getDouble(1);
            JSONObject enitites = twitterPost.getJSONObject("entities");
            JSONArray hashtagsList = (JSONArray) enitites.get("hashtags");
            JSONObject hashObj;
            String hashString = "";
            if (hashtagsList.length() > 0) {
                for (int i = 0; i < hashtagsList.length(); i++) {
                    hashObj = hashtagsList.getJSONObject(i);
                    String hashtagName = hashObj.getString("text");
                    hashString += hashtagName + ",";
                }



            } else {
                hashString = " ";
            }

            Tweet tweet = new Tweet();
            tweet.setCreatedAt(created_at);
            tweet.setTweetId(tweet_id);
            tweet.setHashtags(hashString);
            tweet.setScreenName(screen_name);
            tweet.setLatitude(coordinates[0]);
            tweet.setLongitude(coordinates[1]);
            tweet.setUserId(user_id);
            tweet.setText(yetAnotherFixedTweeet);
            tweet.setFollowersCount(follower_count);
            tweet.setLang(language);
            tweet.setSource(source);

            Indexer indexer = new Indexer();
            indexer.indexTweet(tweet);



        }
        catch (Exception e) {

        }


    }

    public void parseAndIndexTweet(String tweet) throws IOException, ParseException, java.text.ParseException {

        if(indexClosed)
            writer  = new IndexWriter(Crawler.ramDir, new IndexWriterConfig(Version.LUCENE_45 , new ArabicAnalyzer(Version.LUCENE_45)));

        List<String> hashtags = new ArrayList<String>();
        // Parse Tweets
        JSONObject twitterPost = new JSONObject(tweet);
        JSONObject user = twitterPost.getJSONObject("user");
        String created_at = fixDate(twitterPost.getString("created_at"));
        String tweet_id = twitterPost.getString("id_str");
        String user_id = user.getString("id_str");
        String screen_name = user.getString("screen_name");
        String tweetText = twitterPost.getString("text");
        String fixedTweetText = tweetText.replace('\n', ' ');
        String anotherFixedTweetText = fixedTweetText.replace(",",".");
        String yetAnotherFixedTweeet = anotherFixedTweetText.replaceAll("[\\t\\n\\r]"," ");
        String follower_count = Integer.toString(user.getInt("followers_count"));


        double[] coordinates = new double[2];

        try {
            JSONObject geo = twitterPost.getJSONObject("geo");
            JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
            coordinates[0] = coordsJSON.getDouble(0);
            coordinates[1] = coordsJSON.getDouble(1);
            JSONObject enitites = twitterPost.getJSONObject("entities");
            JSONArray hashtagsList = (JSONArray)enitites.get("hashtags");
            JSONObject hashObj;
            String hashString = "";
            if(hashtagsList.length() > 0) {
                for (int i = 0; i < hashtagsList.length(); i++) {
                    hashObj = hashtagsList.getJSONObject(i);
                    String hashtagName = hashObj.getString("text");
                    hashString += hashtagName + ",";
                }
            }
            else
            {
                hashString = " ";
            }

            addGeoDoc(created_at, tweet_id, user_id, screen_name, yetAnotherFixedTweeet, follower_count, coordinates[0], coordinates[1], hashString);
        }
        catch (Exception e) {

        }

        // Index Tweets
        writer.commit();
        writer.close();
        indexClosed = true;
        //System.out.println("Indexed in Inverted Index!!");

    }

    public Date getTwitterDate(String date) throws ParseException, java.text.ParseException {

        final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
        sf.setLenient(true);

        return sf.parse(date);
    }

    public String fixDate(String date) throws ParseException, java.text.ParseException {
        Date dates = getTwitterDate(date);
        SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        return sdf1.format(dates);
    }


    private void addGeoDoc(String created_at, String tweet_id, String user_id, String screen_name, String yetAnotherFixedTweeet, String follower_count, double lat, double lng , String hashtags) throws IOException {
        Document doc  = new Document();
        doc.add(new TextField("tweet_text" , yetAnotherFixedTweeet , Field.Store.YES));
        doc.add(new StringField("created_at" , created_at , Field.Store.YES));
        doc.add(new StringField("tweet_id" , tweet_id , Field.Store.YES));
        doc.add(new StringField("user_id" , user_id , Field.Store.YES));
        doc.add(new StringField("screen_name" , screen_name , Field.Store.YES));
        doc.add(new StringField("followers_count" , follower_count , Field.Store.YES));
        doc.add(new DoubleField("lat" , lat , Field.Store.YES));
        doc.add(new DoubleField("lng" , lng , Field.Store.YES));
        doc.add(new StringField("hashtags" , hashtags , Field.Store.YES));
        writer.addDocument(doc);
        //System.out.println("Indexed DOC");
    }



    public void flushIndex(Directory ramDirectory)
    {
        try {
            Commons commons = new Commons();
            Directory dir = FSDirectory.open(new File(commons.getTweetFlushDir()));
            IndexWriter writer = new IndexWriter(dir , new IndexWriterConfig(Version.LUCENE_45 , new ArabicAnalyzer(Version.LUCENE_45)));
            writer.addIndexes(ramDirectory);
            writer.forceMerge(1);
            writer.close();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
