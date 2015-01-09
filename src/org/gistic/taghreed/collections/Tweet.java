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
	public long tweetID;
	public long userID;
	public String screenName;
	public String tweetText;
	public double lat;
	public double lon;
	public int followersCount;
	public String language;
	public String osystem;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private int priorityValue;

	public Tweet(String created_at, long tweetID, long userID,
			String screenName, String tweetText, int followersCount,
			String language, String osystem, double lat, double lon) {

		this.created_at = created_at;
		this.tweetID = tweetID;
		this.userID = userID;
		this.screenName = screenName;
		this.tweetText = tweetText;
		this.lat = lat;
		this.lon = lon;
		this.followersCount = followersCount;
		this.language = language;
		this.osystem = osystem;
	}
	
	public Tweet(String created_at, String tweetID, String userID,
			String screenName, String tweetText, int followersCount,
			String language, String osystem, String lat, String lon) {

		this.created_at = created_at;
		this.tweetID = Long.parseLong(tweetID);
		this.userID = Long.parseLong(userID);
		this.screenName = screenName;
		this.tweetText = tweetText;
		this.lat = Double.parseDouble(lat);
		this.lon = Double.parseDouble(lon);
		this.followersCount = followersCount;
		this.language = language;
		this.osystem = osystem;
	}

	public Tweet(String created_at, String tweet_id, String user_id,
			String screen_name, String tweet_text, String splitCoordinates,
			String splitCoordinates2, int follower_count) {
		this.created_at = created_at;
		this.tweetID = Long.parseLong(tweet_id);
		this.userID = Long.parseLong(user_id);
		this.screenName = screen_name;
		this.tweetText = tweet_text;
		this.lat = Double.parseDouble(splitCoordinates);
		this.lon = Double.parseDouble(splitCoordinates2);
		this.followersCount = follower_count;
	}

	public Tweet(String tweetLine) throws ParseException {
		String[] token = tweetLine.split(",");
		try {
			this.created_at = parseTweetTimeToString(token[0]);
			this.tweetID = Long.parseLong(token[1]);
			this.userID = Long.parseLong(token[2]);
			this.screenName = token[3];
			this.tweetText = token[4];
			this.followersCount = Integer.parseInt(token[5]);
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
		this.tweetID = arg0.tweet_id;
		this.userID = arg0.user_id;
		this.screenName = arg0.screen_name;
		this.tweetText = arg0.tweet_text;
		this.lat = arg0.x;
		this.lon = arg0.y;
		this.followersCount = arg0.follower_count;
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
		this.priorityValue = priorityValue;
	}
	
	public int getPriorityValue() {
		return priorityValue;
	}

	@Override
	public String toString() {
		return this.created_at + "," + this.tweetID + "," + this.userID + ","
				+ this.screenName + "," + this.tweetText + ","
				+ this.followersCount + "," + this.language + ","
				+ this.osystem + "," + this.lat + "," + this.lon;
	}

	@Override
	public int compareTo(Tweet arg0) {
		return arg0.priorityValue - priorityValue;
	}

}
