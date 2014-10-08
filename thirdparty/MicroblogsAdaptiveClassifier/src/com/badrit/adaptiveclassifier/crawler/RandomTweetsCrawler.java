package com.badrit.adaptiveclassifier.crawler;

import java.io.IOException;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;

import com.badrit.adaptiveclassifier.booleanclassifier.BooleanClassifier;
import com.badrit.adaptiveclassifier.models.Tweet;
import com.badrit.adaptiveclassifier.models.TwitterUser;

public class RandomTweetsCrawler {

	Configuration crawlerConfig;

	public RandomTweetsCrawler(Configuration conf) {
		crawlerConfig = conf;
	}

	public void crawlThroughStream() throws TwitterException, IOException {
		StatusListener listener = new StatusListener() {

			int intTweetsCraweledCount = 0;

			public void onStatus(Status status) {
				try {

					long statusDate = System.currentTimeMillis();

					intTweetsCraweledCount++;
					if (intTweetsCraweledCount % 1000 == 0) {
						System.out
								.println("##Random Streams: stream thread collected: "
										+ intTweetsCraweledCount);
					}

					TwitterUser twitterUser = new TwitterUser(status.getUser());

					if (status.getRetweetedStatus() != null)
						status = status.getRetweetedStatus();

					Tweet tweet = new Tweet(status, twitterUser);

					BooleanClassifier.getInstance().classifyTweet(tweet,
							statusDate);

				} catch (Exception e) {
					System.out
							.println("##Random Streams Exception in Tweet parser of Stream thread");
					e.printStackTrace();
					System.out.println(e.getMessage());
				}

			}

			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				// handle deleted statuses
				System.out.println("##Delete: "
						+ statusDeletionNotice.toString());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.err.println("##Random Stream Rate Limit:"
						+ numberOfLimitedStatuses);
			}

			public void onException(Exception ex) {
				System.out.println("##Random Stream error:" + ex.getMessage());
				ex.printStackTrace();
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
				System.out.println(stallWarning);
			}
		};

		ClassifierConfiguration classifierConfig = ClassifierConfiguration
				.getInstance();

		TwitterStream twitterStream = new TwitterStreamFactory(crawlerConfig)
				.getInstance();
		twitterStream.addListener(listener);

		FilterQuery fq = new FilterQuery(0, null,
				classifierConfig.randomQueries, null,
				(classifierConfig.streamLang == null) ? null
						: new String[] { classifierConfig.streamLang });
		System.out.println("running Random stream thread");
		twitterStream.filter(fq);
	}
}
