package com.badrit.adaptiveclassifier.models;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.badrit.adaptiveclassifier.utils.Utils;

import twitter4j.Status;

public class Tweet {

	public Long ID;
	public TwitterUser twitterUser;
	public String source;
	public String country;
	public String place;
	public String coordinates;
	public Long createdAt;
	public ArrayList<EtweetClasses> labelList;
	public EtweetClasses svmLabel;
	public double svmScore;
	public List<String> hashTags = new ArrayList<String>();
	public List<String> mentions = new ArrayList<String>();
	public long retweetCount;
	public String textProcessed;
	public String textOriginal;

	public Tweet(Status twitter4jTweet, TwitterUser twitterUser) throws IOException {
		ID = twitter4jTweet.getId();
		source = Utils.parseSource(twitter4jTweet.getSource());
		country = Utils.parseCountry(twitter4jTweet.getPlace());
		place = Utils.parsePlaceFullName(twitter4jTweet.getPlace());
		coordinates = Utils.parseGeolocation(twitter4jTweet.getGeoLocation());
		createdAt = twitter4jTweet.getCreatedAt().getTime();
		textProcessed = Utils.processTweetTextForIndex(twitter4jTweet.getText());
		textOriginal = Utils.processTweetTextForStore(twitter4jTweet.getText());
		hashTags = Utils.parseHashtags(twitter4jTweet.getHashtagEntities());
		mentions = Utils.parseMentions(twitter4jTweet.getUserMentionEntities());
		retweetCount = twitter4jTweet.getRetweetCount();
		labelList = new ArrayList<EtweetClasses>();
		this.twitterUser = twitterUser;
	}
}
