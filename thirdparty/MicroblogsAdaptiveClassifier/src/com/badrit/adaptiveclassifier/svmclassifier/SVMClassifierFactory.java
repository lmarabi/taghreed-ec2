package com.badrit.adaptiveclassifier.svmclassifier;

import java.io.File;
import java.util.HashMap;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.ParseException;
import java.io.InputStreamReader;

import jnisvmlight.FeatureVector;
import jnisvmlight.SVMLightModel;

import java.io.OutputStreamWriter;
import java.net.MalformedURLException;

import com.badrit.adaptiveclassifier.models.Tweet;
import com.badrit.adaptiveclassifier.models.EtweetClasses;
import com.badrit.adaptiveclassifier.crawler.ClassifierConfiguration;

/**
 * @author telganainy
 * 
 */

public class SVMClassifierFactory {

	private final int THRESHOLD = 6;

	public SVMLightModel model;
	private HashMap<String, Integer> features;
	private BufferedWriter svm_positive_tweets_writer;
	private BufferedWriter svm_negative_tweets_writer;

	private static SVMClassifierFactory instance;

	public static SVMClassifierFactory getInstance() throws IOException,
			ParseException {
		if (instance == null)
			instance = new SVMClassifierFactory();
		return instance;
	}

	private SVMClassifierFactory() throws IOException, ParseException {

		createSVMFolders();

		svm_positive_tweets_writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								new File(
										ClassifierConfiguration.SVM_CLASSIFIER_POSITIVE_OUTPUT_FILE),
								true), "UTF8"));

		svm_negative_tweets_writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								new File(
										ClassifierConfiguration.SVM_CLASSIFIER_NEGATIVE_OUTPUT_FILE),
								true), "UTF8"));

		loadClassifiers(true);
	}

	public void loadClassifiers(boolean forceLoad) throws IOException,
			ParseException {

		if (!forceLoad) {
			boolean isModelCreated = newModelCreated();
			System.out.println("Checking new Models : " + isModelCreated);
			System.out.println("======================================");

			if (!isModelCreated)
				return;
		}

		/*
		 * Reading Features and Models
		 */
		System.out.println("Loading SVM Models : ");
		try {
			readFeaturesFromFiles();
			readModelsFromFiles();

			System.out.println("features Size: " + features.size());
		} catch (Exception e) {
			System.err
					.println("##SVMError: Can't read SVM Models and Features");
		}

		// Flgging that a new classifiers had been read
		modelHasBeenRead();
	}

	@SuppressWarnings("deprecation")
	private void readModelsFromFiles() throws MalformedURLException,
			ParseException {
		model = SVMLightModel.readSVMLightModelFromURL(new java.io.File(
				"SVM//Model//SVM_model").toURL());
	}

	/*
	 * Features are saved in form : feature_number(\t)feature_term
	 */
	private void readFeaturesFromFiles() throws IOException {

		features = new HashMap<String, Integer>();

		HashMap<String, Integer> currMap = new HashMap<String, Integer>();
		BufferedReader in = new BufferedReader(new InputStreamReader(
				new FileInputStream("SVM//Features//features.txt"), "UTF8"));

		String inputLine = in.readLine();
		while (inputLine != null) {
			String[] a = inputLine.split("\t");
			currMap.put(a[1], Integer.parseInt(a[0]));
			inputLine = in.readLine();
		}
		in.close();
		features = currMap;
	}

	private void modelHasBeenRead() throws IOException {
		FileWriter fw = new FileWriter("SVM//newModelCreated.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("false\n");
		bw.close();
	}

	private boolean newModelCreated() throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(
				new FileInputStream("SVM//newModelCreated.txt"), "UTF8"));
		String s = in.readLine();
		in.close();
		if (s.equalsIgnoreCase("true")) {
			return true;
		} else {
			return false;
		}
	}

	public void classifyTweet(Tweet tweet, long classificationDate)
			throws IOException {

		double classificationScore = classify(tweet.textProcessed, model,
				features);

		// if the tweet is classified to relevant with high score
		if (classificationScore > THRESHOLD) {
			tweet.svmLabel = EtweetClasses.RELEVANT;
			svm_positive_tweets_writer.write(tweet.ID + "\t"
					+ tweet.textProcessed.replaceAll("\t", " ") + "\t"
					+ classificationDate + "\n");
			svm_positive_tweets_writer.flush();
		} else {
			tweet.svmLabel = EtweetClasses.NOT_RELEVANT;
			svm_negative_tweets_writer.write(tweet.ID + "\t"
					+ tweet.textProcessed.replaceAll("\t", " ") + "\t"
					+ classificationDate + "\n");
			svm_negative_tweets_writer.flush();
		}
		tweet.svmScore = classificationScore;
	}

	private double classify(String strProcessedText, SVMLightModel currModel,
			HashMap<String, Integer> currFeatureMap) throws IOException {

		ArrayList<Integer> docList = new ArrayList<Integer>();
		String[] a = strProcessedText.split(" +");
		int wordsCounter = 0;
		int missCounter = 0;
		for (String token : a) {
			token = token.trim();
			if (token.trim().length() == 0)
				continue;
			wordsCounter++;
			Integer index = currFeatureMap.get(token);
			if (index != null) {
				if (!docList.contains(index))
					docList.add(index);
			} else {
				missCounter++;
			}
		}

		// construct the feature vector
		Collections.sort(docList);
		int nDims = docList.size() + 1;
		int[] dims = new int[nDims];
		double[] values = new double[nDims];

		int i = 0;
		for (; i < docList.size(); i++) {
			dims[i] = docList.get(i);
			values[i] = 1.0;
		}
		// miss
		double miss = (missCounter / (wordsCounter * 1.0));
		dims[i] = currFeatureMap.size() + 1;
		values[i] = miss;
		FeatureVector labelFeatureVector = new FeatureVector(dims, values);

		// classifiy
		double score = currModel.classify(labelFeatureVector);

		return score;

	}

	private void createSVMFolders() {
		File SVMFolder = new File("SVM");
		SVMFolder.mkdirs();

		File modelsFolder = new File("SVM//Model");
		modelsFolder.mkdirs();

		File featuresFolder = new File("SVM//Features");
		featuresFolder.mkdirs();
	}

}