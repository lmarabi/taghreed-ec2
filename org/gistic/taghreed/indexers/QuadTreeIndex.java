package org.gistic.taghreed.indexers;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.gistic.taghreed.crawler.Crawler;
import org.gistic.taghreed.json.JSONArray;
import org.gistic.taghreed.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class QuadTreeIndex{

    boolean indexClosed = false;
    SpatialContext spatialContext = null ;
    SpatialStrategy strategy = null ;
    RAMDirectory quadRamDirectory = new RAMDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47 , new ArabicAnalyzer(Version.LUCENE_47));

    Rectangle worldBounds = new RectangleImpl(-180 , 180 , -180 ,180 , spatialContext );
    IndexWriter writer;

    {
        try {
            writer = new IndexWriter(Crawler.quadRamDir, iwc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public QuadTreeIndex(){
        this.spatialContext = new SpatialContext(false , null , worldBounds);

        SpatialPrefixTree tree = new QuadPrefixTree(spatialContext , 11);

        this.strategy = new RecursivePrefixTreeStrategy(tree ,"location");


    }

    public void parseAndIndex(List<String> tweets) throws IOException {
        if(indexClosed)
            writer = new IndexWriter(Crawler.quadRamDir , new IndexWriterConfig(Version.LUCENE_47  , new ArabicAnalyzer(Version.LUCENE_47)));
        for(String tweet : tweets)
        {
            JSONObject twitterPost = new JSONObject(tweet);
            JSONObject user = twitterPost.getJSONObject("user");
            String created_at = twitterPost.getString("created_at");
            String tweet_id = twitterPost.getString("id_str");
            String user_id = user.getString("id_str");
            String screen_name = user.getString("screen_name");
            String tweetText = twitterPost.getString("text");
            String fixedTweetText = tweetText.replace('\n', ' ');
            String anotherFixedTweetText = fixedTweetText.replace(",", ".");
            String yetAnotherFixedTweeet = anotherFixedTweetText.replaceAll("[\\t\\n\\r]", " ");
            String follower_count = Integer.toString(user.getInt("followers_count"));


            double[] coordinates = new double[2];


            try {
                JSONObject geo = twitterPost.getJSONObject("geo");
                JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
                coordinates[0] = coordsJSON.getDouble(0);
                coordinates[1] = coordsJSON.getDouble(1);
                //addGeoDoc(created_at, tweet_id, user_id, screen_name, yetAnotherFixedTweeet, follower_count, coordinates[0], coordinates[1]);
            } catch (Exception e) {

            }
        }
        writer.commit();
        writer.close();
        indexClosed = true ;


    }
    public void parseAndIndexTweet(String tweet) throws IOException, ParseException, java.text.ParseException {
        if(indexClosed)
            writer = new IndexWriter(Crawler.quadRamDir , new IndexWriterConfig(Version.LUCENE_47  , new ArabicAnalyzer(Version.LUCENE_47)));

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
            addGeoDoc(created_at, tweet_id, user_id, screen_name, yetAnotherFixedTweeet, follower_count, coordinates[0], coordinates[1] , hashString);
        } catch (Exception e) {

            
        }
        writer.commit();
        writer.close();
        indexClosed = true;
        //System.out.println("Indexed in Quadtree");

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

    private void addGeoDoc(String created_at , String tweet_id , String user_id , String screen_name , String tweetText , String followers_count , double lat , double lng , String hashtags) throws IOException {

        Document doc = new Document();
        doc.add(new TextField("tweet_text" , tweetText , Field.Store.YES));
        doc.add(new StringField("created_at" , created_at , Field.Store.YES));
        doc.add(new StringField("tweet_id" , tweet_id , Field.Store.YES));
        doc.add(new StringField("user_id" , user_id , Field.Store.YES));
        doc.add(new StringField("screen_name" , screen_name , Field.Store.YES));
        doc.add(new StringField("followers_count" , followers_count , Field.Store.YES));
        doc.add(new StringField("hashtags" , hashtags , Field.Store.YES));

        Shape pointShape = spatialContext.makePoint(lat , lng);

        for (IndexableField f : strategy.createIndexableFields(pointShape))
        {
            doc.add(f);

        }

        doc.add(new StoredField("coords" , spatialContext.toString(pointShape)));

        writer.addDocument(doc);
    }

}