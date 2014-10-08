package com.badrit.adaptiveclassifier.booleanclassifier;

import java.io.File;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import com.badrit.adaptiveclassifier.models.Tweet;
import com.badrit.adaptiveclassifier.models.EtweetClasses;
import com.badrit.adaptiveclassifier.crawler.ClassifierConfiguration;

public class BooleanClassifier {

	private RegexClassifier topicClassifier;
	private BufferedWriter boolean_positive_tweets_writer;
	private BufferedWriter boolean_negative_tweets_writer;

	private static BooleanClassifier instance = null;

	public static BooleanClassifier getInstance() throws IOException {
		if (instance == null) {
			instance = new BooleanClassifier();
		}
		return instance;
	}

	private BooleanClassifier() throws IOException {

		topicClassifier = new RegexClassifier(
				ClassifierConfiguration.ADAPTIVE_CLASSIFIER_TOPIC_QUERIES_FILE);

		boolean_positive_tweets_writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								new File(
										ClassifierConfiguration.BOOLEAN_CLASSIFIER_POSITIVE_OUTPUT_FILE),
								true), "UTF8"));

		boolean_negative_tweets_writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								new File(
										ClassifierConfiguration.BOOLEAN_CLASSIFIER_NEGATIVE_OUTPUT_FILE),
								true), "UTF8"));
	}

	public void classifyTweet(Tweet tweet, long classificationDate)
			throws IOException {

		if (topicClassifier.classify(tweet.textProcessed)) {
			tweet.labelList.add(EtweetClasses.MatchTopicQueries);

			boolean_positive_tweets_writer.write(tweet.ID + "\t"
					+ tweet.textProcessed.replaceAll("\t", " ") + "\t"
					+ classificationDate + "\n");
			boolean_positive_tweets_writer.flush();
		} else {
			boolean_negative_tweets_writer.write(tweet.ID + "\t"
					+ tweet.textProcessed.replaceAll("\t", " ") + "\t"
					+ classificationDate + "\n");
			boolean_negative_tweets_writer.flush();
		}
	}
}
