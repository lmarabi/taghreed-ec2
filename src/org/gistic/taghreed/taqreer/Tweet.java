package org.gistic.taghreed.taqreer;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import net.sf.json.*;

/**
 * This class used to represent a tweet
 * @author meshal
 *
 */
public class Tweet implements Serializable {
	public long tweetId;  				//tweet_id 
    public Date createdAt;				//creat_at
    public double longitude;			//longitude of tweet location 
    public double latitude;				//latitude of tweet location
    public long userId;					//user_id
    public String screenName;			//twitter screnn_name for the user
    public ArrayList<String> hashtags = new ArrayList<String>(); //hashtags
    public String text;					//tweet text 


    
    /**
     * Initialize tweet object from twitter JSON format of tweets
     * @param jsonTweet
     * a String in twitter JSON format that represent the tweet
     */
    public Tweet(String jsonTweet) throws Exception
    {    	
    	JSONObject tweet = JSONObject.fromObject(jsonTweet);
        if (tweet.get("id")== null) throw new Exception("Wrong JSON format for twitter \n" + jsonTweet);

        tweetId = tweet.getLong("id");
        parseCreated_at(tweet.getString("created_at"));
        userId = tweet.getJSONObject("user").getLong("id");
        screenName = tweet.getJSONObject("user").getString("screen_name");
        
        
        JSONArray hashtagsArray =tweet.getJSONObject("entities").getJSONArray("hashtags");
        for(int i=0; i<hashtagsArray.size();i++){
        	hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
        }
        
        
        
        if (!tweet.getJSONObject("coordinates").isNullObject())
        {
        	longitude = tweet.getJSONObject("coordinates").getJSONArray("coordinates").getDouble(0);
        	latitude  = tweet.getJSONObject("coordinates").getJSONArray("coordinates").getDouble(1);
        }
        
        // tweet location is a box, so will consider the tweet location as the middle point of this squere
        else 
        {
            JSONArray topLeft = tweet.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates").getJSONArray(0).getJSONArray(0);
            JSONArray bottomRight = tweet.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates").getJSONArray(0).getJSONArray(2);
            
            longitude = (topLeft.getDouble(0) + bottomRight.getDouble(0)) / 2;
            latitude = (topLeft.getDouble(1) + bottomRight.getDouble(1)) / 2;
        }
    }

    
    public Tweet(){
    	
    }

    /**
     * This method is used to parse the creted_at string element to Date object
     * @param date
     * String that represent a date
     * @throws ParseException
     */
    private void parseCreated_at(String date) throws ParseException
    {
    	final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
    	SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.ENGLISH);
    	sf.setLenient(true);
    	createdAt= sf.parse(date);
    }
}
