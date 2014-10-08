package com.badrit.adaptiveclassifier.crawler;

import java.io.IOException;
import java.util.HashSet;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;

import com.badrit.adaptiveclassifier.models.Tweet;
import com.badrit.adaptiveclassifier.models.TwitterUser;
import com.badrit.adaptiveclassifier.svmclassifier.SVMClassifierFactory;

public class ExtendedTermsCrawler {

	private Configuration crawlerConf;

	public ExtendedTermsCrawler(Configuration conf) {
		crawlerConf = conf;
	}

	public TwitterStream twitterStream;

	public void crawlThroughStream(HashSet<String> expansionSet)
			throws TwitterException, IOException {

		StatusListener listener = new StatusListener() {

			int intTweetsCraweledCount = 0;

			public void onStatus(Status status) {
				try {

					long statusDate = System.currentTimeMillis();

					intTweetsCraweledCount++;
					if (intTweetsCraweledCount % 1000 == 0)
						System.out
								.println("##Extended Streams: stream thread collected: "
										+ intTweetsCraweledCount);

					TwitterUser twitterUser = new TwitterUser(status.getUser());
					if (status.getRetweetedStatus() != null)
						status = status.getRetweetedStatus();

					Tweet tweet = new Tweet(status, twitterUser);
					SVMClassifierFactory.getInstance().classifyTweet(tweet,
							statusDate);
				} catch (Exception e) {
					System.out
							.println("##Extended Streams Exception in Tweet parser of Stream thread");
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
				System.err.println("##Extended Stream Rate Limit:"
						+ numberOfLimitedStatuses);
			}

			public void onException(Exception ex) {
				System.out
						.println("##Extended Stream error:" + ex.getMessage());
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

		System.out.println("=+_+_+_+_+_+_+_+_+_+_+______");

		twitterStream = new TwitterStreamFactory(crawlerConf).getInstance();
		twitterStream.addListener(listener);

		String[] expanstionTerms = new String[expansionSet.size()];

		int i = 0;
		for (String str : expansionSet)
			expanstionTerms[i++] = str;

		System.out.println(expanstionTerms.length);
		FilterQuery fq = new FilterQuery(0, null, expanstionTerms, null, null);

		System.out.println("running stream thread");
		twitterStream.filter(fq);

	}
}
