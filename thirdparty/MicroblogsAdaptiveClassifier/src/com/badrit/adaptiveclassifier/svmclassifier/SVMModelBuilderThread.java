package com.badrit.adaptiveclassifier.svmclassifier;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.IOException;

import java.util.Collections;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;

import jnisvmlight.SVMLightModel;
import jnisvmlight.SVMLightInterface;
import jnisvmlight.TrainingParameters;
import jnisvmlight.LabeledFeatureVector;

import com.badrit.adaptiveclassifier.models.TweetEntry;
import com.badrit.adaptiveclassifier.models.ExpansionTerm;
import com.badrit.adaptiveclassifier.crawler.ClassifierConfiguration;
import com.badrit.adaptiveclassifier.crawler.ExtendedTermsCrawlerController;

public class SVMModelBuilderThread implements Runnable {

	/**
	 * number of expansion terms
	 */
	public final static int EXPANSION_SIZE = 50;

	/*
	 * number of multiples of positive tweets to be the size of negative tweets
	 */
	public final static int SVM_NEGATIVE_TRAIN_MULTIPLE_OF_POSITIVE = 10;

	/*
	 * sleep 1 min in case of errors
	 */
	public final static int ERROR_SLEEP_INTERVAL = 1000 * 60;

	/*
	 * Mininum number of facet occurences to be considered
	 */
	public final static int FACET_MIN_COUNT = 10;

	/*
	 * flag used to choose whether features from negative tweets are to be
	 * included or not
	 */
	public final static boolean INCLUDE_FEATURES_FROM_NEGATIVE_SAMPLES = false;

	/**
	 * 
	 */
	public final static int POSITIVE_LABEL = 1;

	/**
	 * 
	 */
	public final static int NEGATIVE_LABEL = -1;

	/**
	 * 
	 */
	private static final int MILLISECONDS_IN_HOUR = 1000 * 60 * 60;

	public void run() {

		while (true) {
			try {

				long startTime = System.currentTimeMillis();

				ArrayList<String> positiveTweetsText = new ArrayList<String>();
				HashMap<String, Long> positiveFacets = new HashMap<String, Long>();
				HashMap<String, HashSet<Long>> termsInvertedIndex = new HashMap<String, HashSet<Long>>();

				/*
				 * get positive facets and tweets
				 */
				long positiveDocsCount = constructPositiveFacetCountAndInvertedIndex(
						positiveFacets,
						termsInvertedIndex,
						positiveTweetsText,
						ClassifierConfiguration.BOOLEAN_CLASSIFIER_POSITIVE_OUTPUT_FILE,
						startTime);

				/*
				 * remove facets with occurance less than FACET_MIN_COUNT
				 * threshold
				 */
				filterFacetsLessThanMinCount(positiveFacets);
				System.out.println("Constructed positive facets: "
						+ positiveFacets.size());

				HashMap<String, Long> negativeFacets = new HashMap<String, Long>();
				ArrayList<String> negativeTweetsText = new ArrayList<String>();

				/*
				 * get all negative facets and tweets, processed negative tweets
				 * are a multiple of the number of positive tweets.
				 */
				constructNegativeFacetCountAndInvertedIndex(
						negativeFacets,
						termsInvertedIndex,
						negativeTweetsText,
						ClassifierConfiguration.BOOLEAN_CLASSIFIER_NEGATIVE_OUTPUT_FILE,
						startTime, positiveDocsCount);

				/*
				 * remove facets with occurance less than FACET_MIN_COUNT
				 * threshold
				 */
				filterFacetsLessThanMinCount(negativeFacets);
				System.out.println("Constructed negative  facets: "
						+ negativeFacets.size());

				/*
				 * Get Features From Positive Results reverseFeaturesMap for
				 * logging
				 */
				HashMap<Integer, String> reverseFeaturesMap = new HashMap<Integer, String>();
				HashMap<String, Integer> featuresMap = getFeaturesFromResults(
						positiveFacets, negativeFacets, reverseFeaturesMap);

				/*
				 * Get Expansion Terms From Positive Samples To Be Used As
				 * Exclusion Terms For Negative Samples
				 */
				long windowTweetsCount = positiveDocsCount
						+ getAllNegativeDocsCount(
								ClassifierConfiguration.BOOLEAN_CLASSIFIER_NEGATIVE_OUTPUT_FILE,
								startTime);

				ArrayList<ExpansionTerm> expansionList = new ArrayList<ExpansionTerm>();
				HashSet<String> expansionSet = getExpansionSet(positiveFacets,
						termsInvertedIndex, windowTweetsCount);

				writeExpansionSet(expansionSet);

				// Logging features
				logFeatures(reverseFeaturesMap);
				logExpansionList(expansionList);

				// Training Data
				ArrayList<LabeledFeatureVector> traindataList = constructTrainingData(
						featuresMap, expansionSet, positiveTweetsText,
						negativeTweetsText);

				/*
				 * Building Classifier Model
				 */
				if (traindataList.size() > 0)
					buildSVMModel(traindataList);

				// Log Time
				System.out.println("Total Time : "
						+ ((System.currentTimeMillis() - startTime) / 1000.0));

				// write to file that a new model had been created
				AnnounceNewModelsCreated();
				SVMClassifierFactory.getInstance().loadClassifiers(false);
				if (!expansionSet.isEmpty())
					ExtendedTermsCrawlerController.getInstance()
							.refreshCrawler(expansionSet);

				// sleeping
				System.out.println("Sleep for: "
						+ ClassifierConfiguration.getInstance().windowsSize
						* MILLISECONDS_IN_HOUR);
				Thread.sleep(ClassifierConfiguration.getInstance().windowsSize
						* MILLISECONDS_IN_HOUR);

			} catch (Exception e) {
				System.err
						.println("##SVM-Builder: error in SVM Builder thread");
				e.printStackTrace();
				SleepErrorInterval();
			}
		}
	}

	/***
	 * Constructs the labeled feature vector to be sent to svm model builder
	 * 
	 * @param featuresMap
	 *            : Maps each feature string to a feature index in the feature
	 *            vector
	 * @param expansionSet
	 *            : The set of extended terms extracted from positive tweets.
	 * @param positiveDocs
	 *            : Tweets text classified as positive from the boolean
	 *            classifier
	 * @param negativeDocs
	 *            : Tweets text classified as negative from the boolean
	 *            classifier
	 * @return ArrayList<LabeledFeatureVector> of all training data labeled
	 */
	private ArrayList<LabeledFeatureVector> constructTrainingData(
			HashMap<String, Integer> featuresMap, HashSet<String> expansionSet,
			ArrayList<String> positiveDocs, ArrayList<String> negativeDocs) {

		ArrayList<LabeledFeatureVector> traindataList = new ArrayList<LabeledFeatureVector>();

		// Positive Training Samples
		for (int positiveIndex = 0; positiveIndex < positiveDocs.size(); positiveIndex++) {
			String docText = positiveDocs.get(positiveIndex);
			LabeledFeatureVector trainingSample = createTrainingSample(docText,
					featuresMap, POSITIVE_LABEL);
			if (trainingSample != null) {
				traindataList.add(trainingSample);
			}
		}

		// // Negative Training Samples
		int excludedSamples = 0;
		for (int negativeIndex = 0; negativeIndex < negativeDocs.size(); negativeIndex++) {
			String docText = negativeDocs.get(negativeIndex);
			if (excludeSample(docText, expansionSet)) {
				excludedSamples++;
				continue;
			}
			LabeledFeatureVector trainingSample = createTrainingSample(docText,
					featuresMap, NEGATIVE_LABEL);
			if (trainingSample != null) {
				traindataList.add(trainingSample);
			}

		}

		System.out.println("Features Number : " + featuresMap.size());
		System.out.println("ResultsPositive: " + positiveDocs.size());
		System.out.println("ResultsNegative: " + negativeDocs.size());
		System.out.println("True Negative : "
				+ (negativeDocs.size() - excludedSamples));

		return traindataList;
	}

	/**
	 * count the number of all negative documents within the window of the
	 * classifier.
	 * 
	 * @param booleanClassifierNegativeOutputFile
	 *            : Path to the the negative output file from boolean classifier
	 * @param startTime
	 *            : SVM building Starting time.
	 * @return number of negative tweets within the training time window.
	 * @throws IOException
	 */
	private long getAllNegativeDocsCount(
			String booleanClassifierNegativeOutputFile, long startTime)
			throws IOException {

		int docCount = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(booleanClassifierNegativeOutputFile),
				"UTF8"));
		String entry;
		while ((entry = br.readLine()) != null) {
			TweetEntry tweet = creatTweetEntry(entry);
			if (startTime - tweet.timeStamp > ClassifierConfiguration
					.getInstance().windowsSize * MILLISECONDS_IN_HOUR)
				continue;
			docCount++;
		}
		br.close();
		return docCount;
	}

	/**
	 * Returns the set of expanstion terms
	 * 
	 * @param poitiveFacets
	 *            : All positive facets during the training time window
	 * @param termsInvertedIndex
	 *            : Terms inverted index mapping each term to the set of
	 *            ducuments that the term appeared in.
	 * @param windowTweetsCount
	 *            : number of tweets in the training time window.
	 * @return
	 */
	private HashSet<String> getExpansionSet(
			HashMap<String, Long> poitiveFacets,
			HashMap<String, HashSet<Long>> termsInvertedIndex,
			long windowTweetsCount) {

		ArrayList<ExpansionTerm> expansionList = new ArrayList<ExpansionTerm>();
		int featureCounter = 1;

		for (String term : poitiveFacets.keySet()) {
			double termFreq = (double) poitiveFacets.get(term);
			double docFreq = (double) termsInvertedIndex.get(term).size();
			double tfidf = termFreq * Math.log10(windowTweetsCount / docFreq);
			System.out.println(featureCounter++ + ") T: " + term + " TF: "
					+ termFreq + " DF: " + docFreq + " N: " + windowTweetsCount
					+ " tf-idf = " + tfidf);
			expansionList.add(new ExpansionTerm(term, tfidf));
		}

		// sort in descending order based on tf-idf value
		Collections.sort(expansionList);
		// Select Top tf-idf words To be excluded From Negative Samples
		HashSet<String> expansionSet = new HashSet<String>();
		for (int i = 0; i < EXPANSION_SIZE && i < expansionList.size(); i++) {
			expansionSet.add(expansionList.get(i).term);
		}
		return expansionSet;
	}

	/**
	 * construct the negative term facets and add terms to the inverted index.
	 * 
	 * @param negativeFacets
	 *            : HashMap to map each term to term frequency.
	 * @param termsInvertedIndex
	 *            : Terms inverted index mapping each term to the set of
	 *            ducuments that the term appeared in.
	 * @param negativeTweetsText
	 *            Set of tweets text in the negative set.
	 * @param booleanClassifierNegativeOutputFile
	 *            : Path to the the negative output file from boolean classifier
	 * @param startTime
	 *            : SVM building Starting time.
	 * @param positiveDocsCount
	 *            number of positive tweets during the training time widnow
	 * @return size of a subset of negative tweets idially will be of size
	 *         positiveDocsCount*SVM_NEGATIVE_TRAIN_MULTIPLE_OF_POSITIVE
	 * @throws IOException
	 */
	private long constructNegativeFacetCountAndInvertedIndex(
			HashMap<String, Long> negativeFacets,
			HashMap<String, HashSet<Long>> termsInvertedIndex,
			ArrayList<String> negativeTweetsText,
			String booleanClassifierNegativeOutputFile, long startTime,
			long positiveDocsCount) throws IOException {

		int docCount = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(booleanClassifierNegativeOutputFile),
				"UTF8"));
		String entry;
		while ((entry = br.readLine()) != null) {
			TweetEntry tweet = creatTweetEntry(entry);
			if (startTime - tweet.timeStamp > ClassifierConfiguration
					.getInstance().windowsSize * MILLISECONDS_IN_HOUR)
				continue;
			docCount++;

			// if collected the desired number of negative tweets .. break
			if (docCount >= positiveDocsCount
					* SVM_NEGATIVE_TRAIN_MULTIPLE_OF_POSITIVE)
				break;
			constructTermFacets(tweet.tweetText, negativeFacets);
			constructInvertedIndex(tweet.tweetText, tweet.id,
					termsInvertedIndex);
			negativeTweetsText.add(tweet.tweetText);
		}
		br.close();
		return docCount;
	}

	/**
	 * filter out facets with occurances less than FACET_MIN_COUNT
	 * 
	 * @param positiveFacets
	 *            : facets set to be filtered
	 */
	private void filterFacetsLessThanMinCount(HashMap<String, Long> facets) {
		ArrayList<String> facetsToBeRemoved = new ArrayList<String>();

		for (String facet : facets.keySet()) {
			if (facets.get(facet) < FACET_MIN_COUNT)
				facetsToBeRemoved.add(facet);
		}
		for (String facet : facetsToBeRemoved)
			facets.remove(facet);
	}

	/**
	 * write expansion set to an external file
	 * 
	 * @param expansionSet
	 * @throws IOException
	 */
	private void writeExpansionSet(HashSet<String> expansionSet)
			throws IOException {
		BufferedWriter bw = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(
								new File(
										ClassifierConfiguration.ADAPTIVE_CLASSIFIER_EXTENDED_QUERIES_FILE)),
						"UTF8"));
		for (String expansionTerm : expansionSet)
			bw.write(expansionTerm + "\n");
		bw.close();
	}

	private void SleepErrorInterval() {
		try {
			Thread.sleep(ERROR_SLEEP_INTERVAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void logExpansionList(ArrayList<ExpansionTerm> expansionList)
			throws IOException {
		System.out.println("\nAfter Sorting Features : \n"
				+ "======================");
		for (int i = 1; i <= expansionList.size() && i <= EXPANSION_SIZE; i++) {
			System.out.println(i + ") " + expansionList.get(i - 1));
		}
	}

	/**
	 * announce a new model has been created
	 * 
	 * @throws IOException
	 */
	private void AnnounceNewModelsCreated() throws IOException {
		FileWriter fw = new FileWriter("SVM//newModelCreated.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("true\n");
		bw.close();

	}

	private void buildSVMModel(ArrayList<LabeledFeatureVector> traindataList) {

		SVMLightInterface trainer = new SVMLightInterface();
		SVMLightInterface.SORT_INPUT_VECTORS = true;
		LabeledFeatureVector[] traindataArray = new LabeledFeatureVector[traindataList
				.size()];
		traindataArray = traindataList.toArray(traindataArray);
		TrainingParameters tp = new TrainingParameters();

		// Switch on some debugging output
		tp.getLearningParameters().verbosity = 1;
		System.out.println("\nTRAINING SVM-light MODEL ..");
		SVMLightModel model = trainer.trainModel(traindataArray, tp);
		System.out.println("model" + model.toString());
		System.out.println(" DONE Building Model.");
		model.writeModelToFile("SVM//Model//SVM_model");
		testModelOnTrainingData(model, traindataArray, trainer);
	}

	private void testModelOnTrainingData(SVMLightModel model,
			LabeledFeatureVector[] traindata, SVMLightInterface trainer) {
		int N = traindata.length;
		System.out.println("\nVALIDATING SVM-light MODEL in Java..");
		int precision = 0;
		for (int i = 0; i < N; i++) {

			// Classify a test vector using the Java object
			// (in a real application, this should not be one of the training
			// vectors)
			double d = model.classify(traindata[i]);
			if ((traindata[i].getLabel() < 0 && d < 0)
					|| (traindata[i].getLabel() > 0 && d > 0)) {
				precision++;
			}

		}
		System.out.println(" DONE Testing Model on Training Data.");
		System.out.println("\n" + ((double) precision / N)
				+ " PRECISION=RECALL ON  TRAINING SET.");

		System.out.println("\nVALIDATING SVM-light MODEL in Native Mode..");
		precision = 0;
		for (int i = 0; i < N; i++) {

			// Classify a test vector using the Native Interface
			// (in a real application, this should not be one of the training
			// vectors)
			double d = trainer.classifyNative(traindata[i]);
			if ((traindata[i].getLabel() < 0 && d < 0)
					|| (traindata[i].getLabel() > 0 && d > 0)) {
				precision++;
			}
		}
		System.out.println(" DONE.");
		System.out.println("\n" + ((double) precision / N)
				+ " PRECISION=RECALL ON  TRAINING SET.");

	}

	private LabeledFeatureVector createTrainingSample(String text,
			HashMap<String, Integer> featuresMap, int label) {

		text = text.trim();
		if (text.length() == 0)
			return null;

		String[] tokens = text.split(" +");
		if (tokens.length == 0)
			return null;
		ArrayList<Integer> tweetFeatures = getFeaturesFromTweet(featuresMap,
				tokens);
		double missFeature = getFeatureMissRate(featuresMap, tokens);
		int missFeatureIndex = featuresMap.size() + 1;
		LabeledFeatureVector labeledFeatureVector = createLabeledFeatureVector(
				tweetFeatures, missFeature, missFeatureIndex, label);
		return labeledFeatureVector;
	}

	/**
	 * check whether to exclude a negative tweet if it contains a token that
	 * appeared in the expansion set.
	 * 
	 * @param tweetText
	 *            : Tweet text under inspection
	 * @param expansionSet
	 *            : the set of expansion terms.
	 * @return
	 */
	private boolean excludeSample(String tweetText, HashSet<String> expansionSet) {
		String[] tokens = tweetText.split(" +");
		for (String token : tokens) {
			token = token.trim();
			if (expansionSet.contains(token))
				return true;
		}
		return false;
	}

	private LabeledFeatureVector createLabeledFeatureVector(
			ArrayList<Integer> tweetFeatures, double missFeature,
			int missFeatureIndex, int label) {
		int nDims = tweetFeatures.size() + 1;
		int[] dims = new int[nDims];
		double[] values = new double[nDims];

		int i = 0;
		for (; i < tweetFeatures.size(); i++) {
			dims[i] = tweetFeatures.get(i);
			values[i] = 1.0;
		}
		// miss
		dims[i] = missFeatureIndex;
		values[i] = missFeature;
		LabeledFeatureVector labelFeatureVector = new LabeledFeatureVector(
				label, dims, values);
		labelFeatureVector.normalizeL2();
		return labelFeatureVector;
	}

	private double getFeatureMissRate(HashMap<String, Integer> featuresMap,
			String[] tokens) {
		int wordsCounter = 0;
		int missCounter = 0;
		for (String token : tokens) {
			token = token.trim();
			if (token.length() == 0)
				continue;
			wordsCounter++;
			if (featuresMap.get(token) == null)
				missCounter++;
		}
		return (missCounter / (wordsCounter * 1.0));
	}

	private ArrayList<Integer> getFeaturesFromTweet(
			HashMap<String, Integer> featuresMap, String[] tokens) {

		ArrayList<Integer> tweetFeatures = new ArrayList<Integer>();
		for (String token : tokens) {
			token = token.trim();
			if (token.length() == 0)
				continue;

			Integer index = featuresMap.get(token);
			if (index != null)
				if (!tweetFeatures.contains(index))
					tweetFeatures.add(index);

		}
		Collections.sort(tweetFeatures);
		return tweetFeatures;
	}

	private void logFeatures(HashMap<Integer, String> reverseFeaturesMap)
			throws IOException {

		BufferedWriter facetBuffWriter = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(
						"SVM//Features//features.txt"), "UTF-8"));
		for (int i = 1; i <= reverseFeaturesMap.size(); i++) {
			String featureTerm = reverseFeaturesMap.get(i);
			facetBuffWriter.write(i + "\t" + featureTerm + "\n");
		}
		facetBuffWriter.close();

	}

	private HashMap<String, Integer> getFeaturesFromResults(
			HashMap<String, Long> positiveFacets,
			HashMap<String, Long> negativeFacets,
			HashMap<Integer, String> reverseFeaturesMap) {
		HashMap<String, Integer> featuresMap = new HashMap<String, Integer>();
		int featureCounter = 1;

		// construct features map to give each feature text an id
		for (String facet : positiveFacets.keySet()) {
			featuresMap.put(facet, featureCounter);
			reverseFeaturesMap.put(featureCounter++, facet);
		}
		int positiveFeatures = featuresMap.size();

		// Top Words of Negative Samples
		if (INCLUDE_FEATURES_FROM_NEGATIVE_SAMPLES) { // subject to experiment
			for (String facet : negativeFacets.keySet()) {
				if (!featuresMap.containsKey(facet)) {
					featuresMap.put(facet, featureCounter);
					reverseFeaturesMap.put(featureCounter++, facet);
				}
			}
		}

		// logging
		System.out.println("Features");
		System.out.println("\tFrom Positive Samples:" + positiveFeatures);
		System.out.println("\tFrom Negative Samples:"
				+ (featuresMap.size() - positiveFeatures));

		return featuresMap;

	}

	/**
	 * construct the positive term facets and add terms to the inverted index.
	 * 
	 * @param facets
	 *            : HashMap to map each term to term frequency.
	 * @param invertedIndex
	 *            : Terms inverted index mapping each term to the set of
	 *            documents that the term appeared in.
	 * @param positiveTweetsText
	 *            Set of tweets text in the positive set.
	 * @param classifierOutputFilePath
	 *            : Path to the the positive output file from boolean classifier
	 * @param startTime
	 *            : SVM building Starting time.
	 * @return size of all positive tweets during the training time window
	 * @throws IOException
	 */
	private long constructPositiveFacetCountAndInvertedIndex(
			HashMap<String, Long> facets,
			HashMap<String, HashSet<Long>> invertedIndex,
			ArrayList<String> positiveTweetsText,
			String classifierOutputFilePath, long startTime)
			throws IOException, FileNotFoundException {
		int docCount = 0;
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(classifierOutputFilePath), "UTF8"));
		String entry;
		while ((entry = br.readLine()) != null) {
			TweetEntry tweet = creatTweetEntry(entry);
			if (startTime - tweet.timeStamp > ClassifierConfiguration
					.getInstance().windowsSize * MILLISECONDS_IN_HOUR)
				continue;
			docCount++;
			constructTermFacets(tweet.tweetText, facets);
			constructInvertedIndex(tweet.tweetText, tweet.id, invertedIndex);
			positiveTweetsText.add(tweet.tweetText);
		}
		br.close();
		return docCount;
	}

	private void constructInvertedIndex(String tweetText, long tweetID,
			HashMap<String, HashSet<Long>> invertedIndex) {
		String[] splitted = tweetText.split(" +");
		for (String token : splitted) {
			if (!invertedIndex.containsKey(token))
				invertedIndex.put(token, new HashSet<Long>());
			invertedIndex.get(token).add(tweetID);
		}
	}

	private void constructTermFacets(String tweetText,
			HashMap<String, Long> facets) {
		String[] splitted = tweetText.split(" +");
		for (String token : splitted) {
			if (!facets.containsKey(token))
				facets.put(token, 0l);
			long currCount = facets.get(token);
			facets.put(token, currCount + 1);
		}
	}

	private TweetEntry creatTweetEntry(String entry) {
		// A Tweet should be splitted based on the tab
		// into an id, tweet and crawl/classification timestamp
		String[] splitted = entry.split("\\t");
		long id = Long.parseLong(splitted[0]);
		String tweetText = splitted[1];
		long timeStamp = Long.parseLong(splitted[2]);
		return new TweetEntry(tweetText, id, timeStamp);
	}

	public static void main(String[] args) {
		(new Thread(new SVMModelBuilderThread())).start();
	}
}
