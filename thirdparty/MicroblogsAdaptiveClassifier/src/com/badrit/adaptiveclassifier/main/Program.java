package com.badrit.adaptiveclassifier.main;

import java.util.List;
import java.io.IOException;
import java.text.ParseException;

import twitter4j.TwitterException;
import twitter4j.conf.Configuration;

import com.badrit.adaptiveclassifier.crawler.TopicTweetsCrawler;
import com.badrit.adaptiveclassifier.crawler.RandomTweetsCrawler;
import com.badrit.adaptiveclassifier.crawler.ClassifierConfiguration;
import com.badrit.adaptiveclassifier.svmclassifier.SVMModelBuilderThread;

public class Program {

	/**
	 * List of twitter4j configurations (tokens/proxies)
	 */
	public static List<Configuration> lstTwitter4jConfigurations;

	/**
	 * Topics tweets crawler configuration
	 */
	public static Configuration objTopicTweetsCrawlerConfiguration;

	/**
	 * Random tweets crawler configuration
	 */
	public static Configuration objRandomTweetsCrawlerConfiguration;

	/**
	 * Extended tweets crawler configuration
	 */
	public static Configuration objExtendedTweetsCrawlerConfiguration;

	/**
	 * Start 2 twitter streaming crawlers: 1) stream positive tweets using the
	 * topics provided 2) stream random tweets
	 * 
	 * @throws TwitterException
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void startCrawlerThreads() throws TwitterException,
			IOException, ParseException {

		// start Boolean queries crawler
		new TopicTweetsCrawler(objTopicTweetsCrawlerConfiguration)
				.crawlThroughStream();

		// start random queries crawler
		new RandomTweetsCrawler(objRandomTweetsCrawlerConfiguration)
				.crawlThroughStream();

	}

	public static void main(String[] args) throws TwitterException,
			IOException, ParseException {

		// read configurations, proxies and twitter tokens.
		lstTwitter4jConfigurations = ClassifierConfiguration.getInstance().crawlersConfigurationList;
		
		//Some users might find it convinient to run on a single machine 
		//with one crawler so we modified the code in order to adhere to that scenario
		if(lstTwitter4jConfigurations.size() == 1){
			objTopicTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(0);
			objRandomTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(0);
			objExtendedTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(0);
		}else if(lstTwitter4jConfigurations.size() == 2){
			//This proposed setting assigns the random tweet crawler and the extended tweet
			//crawler to the second entry in the configuration file
			objTopicTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(0);
			objRandomTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(1);
			objExtendedTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(1);
		}else{
			objTopicTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(0);
			objRandomTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(1);
			objExtendedTweetsCrawlerConfiguration = lstTwitter4jConfigurations.get(2);
		}

		// Start the first two crawlers (positive, random)
		startCrawlerThreads();

		// start svm model builder
		new Thread(new SVMModelBuilderThread()).start();
	}
}
