package com.badrit.adaptiveclassifier.crawler;

import twitter4j.Status;
import java.io.IOException;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.TwitterStream;
import twitter4j.StatusListener;
import java.text.ParseException;

import twitter4j.TwitterException;
import twitter4j.conf.Configuration;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStreamFactory;

import com.badrit.adaptiveclassifier.models.Tweet;
import com.badrit.adaptiveclassifier.models.TwitterUser;
import com.badrit.adaptiveclassifier.svmclassifier.SVMClassifierFactory;
import com.badrit.adaptiveclassifier.booleanclassifier.BooleanClassifier;

public class TopicTweetsCrawler {

	private BooleanClassifier booleanClassifier;
	private SVMClassifierFactory svmClassifier;

	Configuration crawlerConfig;

	public TopicTweetsCrawler(Configuration conf) {
		crawlerConfig = conf;
	}

	int intTweetsCraweledCount = 0;

	public void crawlThroughStream() throws TwitterException, IOException,
			ParseException {

		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				try {
					long statusDate = System.currentTimeMillis();

					intTweetsCraweledCount++;
					if (intTweetsCraweledCount % 1000 == 0) {
						System.out
								.println("##Topic Streams: stream thread collected: "
										+ intTweetsCraweledCount);
					}

					// TwitterUser and other tweet data are augmented for future
					// use. if not needed can be removed.
					TwitterUser twitterUser = new TwitterUser(status.getUser());

					// If status is retweet write the source tweet
					if (status.getRetweetedStatus() != null)
						status = status.getRetweetedStatus();
					Tweet tweet = new Tweet(status, twitterUser);

					// Ise the boolean classifier to classify the crawled tweet
					booleanClassifier.classifyTweet(tweet, statusDate);
					// if there exists a trained svm model, use it
					if (svmClassifier.model != null) {
						svmClassifier.classifyTweet(tweet, statusDate);
					}

				} catch (Exception e) {
					System.out
							.println("##Topic Streams Exception in Tweet parser of Stream thread");
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
				System.err.println("##Topic Stream Rate Limit:"
						+ numberOfLimitedStatuses);
			}

			public void onException(Exception ex) {
				System.out.println("##Topic Stream error:" + ex.getMessage());
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

		booleanClassifier = BooleanClassifier.getInstance();
		svmClassifier = SVMClassifierFactory.getInstance();
		ClassifierConfiguration classifierConfig = ClassifierConfiguration
				.getInstance();

		TwitterStream twitterStream = new TwitterStreamFactory(crawlerConfig)
				.getInstance();
		twitterStream.addListener(listener);

		FilterQuery fq = new FilterQuery(0, null,
				classifierConfig.topicQueries, null,
				(classifierConfig.streamLang == null) ? null
						: new String[] { classifierConfig.streamLang });

		System.out.println("running Topic stream thread");
		twitterStream.filter(fq);
	}
}
