package org.gistic.taghreed.collections;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by saifalharthi on 5/19/14.
 */
public class Tweet {

	public String created_at;
	public String tweetID;
	public String userID;
	public String screenName;
	public String tweetText;
	public String lat;
	public String lon;
	public int followersCount;
	public String language;
	public String osystem;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private int priorityValue;

	public Tweet(String created_at, String tweetID, String userID,
			String screenName, String tweetText, int followersCount,
			String language, String osystem, String lat, String lon) {

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

	public Tweet(String created_at, String tweet_id, String user_id,
			String screen_name, String tweet_text, String latitude,
			String longitude, int follower_count) {
		this.created_at = created_at;
		this.tweetID = tweet_id;
		this.userID = user_id;
		this.screenName = screen_name;
		this.tweetText = tweet_text;
		this.lat = latitude;
		this.lon = longitude;
		this.followersCount = follower_count;
	}

	public Tweet(String tweetLine) throws ParseException {
		String[] token = tweetLine.split(",");
		try {
			this.created_at = parseTweetTimeToString(token[0]);
			this.tweetID = token[1];
			this.userID = token[2];
			this.screenName = token[3];
			this.tweetText = token[4];
			this.followersCount = Integer.parseInt(token[5]);
			if (token.length > 8) {
				this.language = token[6];
				this.osystem = token[7];
				this.lat = token[8];
				this.lon = token[9];
			} else {
				this.lat = token[6];
				this.lon = token[7];
			}
		} catch (Exception e) {
			System.out.println(tweetLine + "\nCSV number= " + token.length);
		}
	}

	public static String parseTweetTimeToString(String expr)
			throws ParseException {
		String[] datetemp = expr.split(" ");
		Date date = sdf.parse(datetemp[0]);
		return sdf.format(date);
	}

	public static Date parseTweetTimeToDate(String expr) throws ParseException {
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

}
