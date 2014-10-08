package com.badrit.adaptiveclassifier.crawler;

import java.util.HashSet;
import java.io.IOException;
import twitter4j.TwitterException;

import com.badrit.adaptiveclassifier.main.Program;

public class ExtendedTermsCrawlerController {

	private static ExtendedTermsCrawlerController instance;
	private ExtendedTermsCrawler extendedTermsCrawler;

	public static ExtendedTermsCrawlerController getInstance() {
		if (instance == null)
			instance = new ExtendedTermsCrawlerController();
		return instance;
	}

	public void refreshCrawler(HashSet<String> expansionSet)
			throws IOException, TwitterException {
		System.out.println("-=-=-=-=: Refreshing extendedSet");
		if (extendedTermsCrawler != null)
			extendedTermsCrawler.twitterStream.cleanUp();
		extendedTermsCrawler = new ExtendedTermsCrawler(
				Program.objExtendedTweetsCrawlerConfiguration);
		extendedTermsCrawler.crawlThroughStream(expansionSet);
	}
}