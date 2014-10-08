package com.badrit.adaptiveclassifier.models;

import java.util.Date;

import com.badrit.adaptiveclassifier.utils.Utils;

import twitter4j.User;

public class TwitterUser {

	public Long ID;
	public String name;
	public String screenName;
	public String bigProfileImageURL;
	public String timeZone;
	public int followersCount;
	public int followeesCount;
	public String location;
	public String language;
	public boolean isVerified;
	public Date createdAt;
	public String description;
	public int favouritesCount;
	public int statusesCount;
	public String userURL;

	public TwitterUser(User twitter4jUser) {
		extractFromTwitter4jUser(twitter4jUser);
	}

	public void extractFromTwitter4jUser(User twitter4jUser) {
		ID = twitter4jUser.getId();
		name = Utils.processTweetTextForStore(twitter4jUser.getName());
		screenName = Utils.processTweetTextForStore(twitter4jUser.getScreenName());
		bigProfileImageURL = twitter4jUser.getBiggerProfileImageURL();
		timeZone = twitter4jUser.getTimeZone();
		followersCount = twitter4jUser.getFollowersCount();
		followeesCount = twitter4jUser.getFriendsCount();
		location = twitter4jUser.getLocation();
		language = twitter4jUser.getLang();
		isVerified = twitter4jUser.isVerified();
		createdAt = twitter4jUser.getCreatedAt();
		description = twitter4jUser.getDescription();
		favouritesCount = twitter4jUser.getFavouritesCount();
		statusesCount = twitter4jUser.getStatusesCount();
		userURL = twitter4jUser.getURL();
	}
}
