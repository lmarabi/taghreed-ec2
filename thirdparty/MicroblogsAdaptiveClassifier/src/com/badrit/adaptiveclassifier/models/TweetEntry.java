package com.badrit.adaptiveclassifier.models;

public class TweetEntry {
	public String tweetText;
	public long id;
	public long timeStamp;
	public TweetEntry(String tweetText, long id, long timeStamp) {
		this.tweetText = tweetText;
		this.id = id;
		this.timeStamp = timeStamp;
	}
	
}
