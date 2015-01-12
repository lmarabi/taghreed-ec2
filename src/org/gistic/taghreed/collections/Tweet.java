package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.gistic.taghreed.spatialHadoop.Tweets;

/**
 * Created by saifalharthi on 5/19/14.
 */
public class Tweet implements Comparable<Tweet>{

	public String created_at;
	public long tweet_id;
	public long user_id;
	public String screen_name;
	public String tweet_text;
	public double lat;
	public double lon;
	public int follower_count;
	public String language;
	public String osystem;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	public int priority;

	public Tweet(String created_at, long tweetID, long userID,
			String screenName, String tweetText, int followersCount,
			String language, String osystem, double lat, double lon) {

		this.created_at = created_at;
		this.tweet_id = tweetID;
		this.user_id = userID;
		this.screen_name = screenName;
		this.tweet_text = tweetText;
		this.lat = lat;
		this.lon = lon;
		this.follower_count = followersCount;
		this.language = language;
		this.osystem = osystem;
	}
	
	public Tweet(String created_at, String tweetID, String userID,
			String screenName, String tweetText, int followersCount,
			String language, String osystem, String lat, String lon) {

		this.created_at = created_at;
		this.tweet_id = Long.parseLong(tweetID);
		this.user_id = Long.parseLong(userID);
		this.screen_name = screenName;
		this.tweet_text = tweetText;
		this.lat = Double.parseDouble(lat);
		this.lon = Double.parseDouble(lon);
		this.follower_count = followersCount;
		this.language = language;
		this.osystem = osystem;
	}

	public Tweet(String created_at, String tweet_id, String user_id,
			String screen_name, String tweet_text, String splitCoordinates,
			String splitCoordinates2, int follower_count) {
		this.created_at = created_at;
		this.tweet_id = Long.parseLong(tweet_id);
		this.user_id = Long.parseLong(user_id);
		this.screen_name = screen_name;
		this.tweet_text = tweet_text;
		this.lat = Double.parseDouble(splitCoordinates);
		this.lon = Double.parseDouble(splitCoordinates2);
		this.follower_count = follower_count;
	}

	public Tweet(String tweetLine) throws ParseException {
		String[] token = tweetLine.split(",");
		try {
			this.created_at = token[0];//parseTweetTimeToString(token[0]);
			this.tweet_id = Long.parseLong(token[1]);
			this.user_id = Long.parseLong(token[2]);
			this.screen_name = token[3];
			this.tweet_text = token[4];
			this.follower_count = Integer.parseInt(token[5]);
			if (token.length > 8) {
				this.language = token[6];
				this.osystem = token[7];
				this.lat = Double.parseDouble(token[8]);
				this.lon = Double.parseDouble(token[9]);
			} else {
				this.lat = Double.parseDouble(token[6]);
				this.lon = Double.parseDouble(token[7]);
			}
		} catch (Exception e) {
			System.out.println(tweetLine + "\nCSV number= " + token.length);
		}
	}

	public Tweet(Tweets arg0) {
		this.created_at = arg0.created_at;
		this.tweet_id = arg0.tweet_id;
		this.user_id = arg0.user_id;
		this.screen_name = arg0.screen_name;
		this.tweet_text = arg0.tweet_text;
		this.lat = arg0.x;
		this.lon = arg0.y;
		this.follower_count = arg0.follower_count;
		this.language = arg0.language;
		this.osystem = arg0.osystem;
	}

	public static String parseTweetTimeToString(String expr)
			throws ParseException {
		String[] datetemp = expr.split(" ");
		Date date = sdf.parse(datetemp[0]);
		return sdf.format(date);
	}

	public synchronized static Date parseTweetTimeToDate(String expr) throws ParseException {
		String[] datetemp = expr.split(" ");
		return sdf.parse(datetemp[0]);
	}
	
	public void setPriorityValue(int priorityValue) {
		this.priority = priorityValue;
	}
	
	public int getPriorityValue() {
		return priority;
	}

	@Override
	public String toString() {
		return this.created_at + "," + this.tweet_id + "," + this.user_id + ","
				+ this.screen_name + "," + this.tweet_text + ","
				+ this.follower_count + "," + this.language + ","
				+ this.osystem + "," + this.lat + "," + this.lon;
	}

	@Override
	public int compareTo(Tweet arg0) {
		return arg0.priority - priority;
	}

}
