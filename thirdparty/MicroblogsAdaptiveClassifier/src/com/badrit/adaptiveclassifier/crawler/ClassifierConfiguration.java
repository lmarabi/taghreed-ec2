package com.badrit.adaptiveclassifier.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class ClassifierConfiguration {

	public int windowsSize;
	public String streamLang;
	public Configuration twitterStreamConfiguration;
	private String twitterConsumerKey;
	private String twitterConsumerSecret;
	private String twitterAccessToken;
	private String twitterAccessTokenSecret;

	public static final String ADAPTIVE_CLASSIFIER_TOPIC_QUERIES_FILE = "resources//topic_queries.txt";
	public static final String ADAPTIVE_CLASSIFIER_RANDOM_QUERIES_FILE = "resources//random_queries.txt";
	public static final String ADAPTIVE_CLASSIFIER_EXTENDED_QUERIES_FILE = "resources//extended_queries.txt";
	
	public static final String BOOLEAN_CLASSIFIER_POSITIVE_OUTPUT_FILE = "boolean_positive_tweets.txt";
	public static final String BOOLEAN_CLASSIFIER_NEGATIVE_OUTPUT_FILE = "boolean_negative_tweets.txt";
	public static final String SVM_CLASSIFIER_POSITIVE_OUTPUT_FILE = "svm_positive_tweets.txt";
	public static final String SVM_CLASSIFIER_NEGATIVE_OUTPUT_FILE = "svm_negative_tweets.txt";
	
	private final String TWITTER_TOKENS_CONFIG_FILE = "resources//twitterTokens.config";
	private final String PROXIES_CONFIG_FILE = "resources//proxies.config";

	private static final String ADAPTIVE_CLASSIFIER_CONFIG_FILE = "resources//classifier.config";
	
	private final String CLASSIFIER_WINDOW = "classifier.window";
	private final String TWITTER_STREAM_LANG = "query.lang";
	
	
	public ArrayList<ProxyServer> proxyServersList;
	public ArrayList<OAuth> twitterAuthenticationTokens;
	public ArrayList<Configuration> crawlersConfigurationList;
	
	String[] topicQueries;
	String[] randomQueries;


	private static ClassifierConfiguration configurationInstance;

	private ClassifierConfiguration() throws IOException {
		this.loadProxies();
		this.loadTwitterTokens();
		this.setupCrawlersConfigurations();
		configure();
	}

	public static ClassifierConfiguration getInstance() throws IOException {
		if (configurationInstance == null)
			configurationInstance = new ClassifierConfiguration();
		return configurationInstance;
	}

	private void configure() throws IOException {

		// Read the properties
		Properties configurationProperties = new Properties();
		FileInputStream fileInputStream = new FileInputStream(ADAPTIVE_CLASSIFIER_CONFIG_FILE);
		configurationProperties.load(fileInputStream);

		// Extract classifier parameters
		this.windowsSize = Integer.parseInt(configurationProperties.getProperty(CLASSIFIER_WINDOW, "24"));
		this.streamLang = configurationProperties.getProperty(TWITTER_STREAM_LANG, null);


		// Read predefined topic queries
		topicQueries = readTopicQueries();
		randomQueries = readRandomQueries();
	}

	private String[] readTopicQueries() throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				ADAPTIVE_CLASSIFIER_TOPIC_QUERIES_FILE), "UTF8"));
		String q;
		ArrayList<String> queries = new ArrayList<String>();
		while ((q = br.readLine()) != null)
			queries.add(q);
		br.close();
		
		String[] queriesArr = new String[queries.size()];
		for (int i = 0; i < queriesArr.length; i++) {
			queriesArr[i] = queries.get(i);
		}
		return queriesArr;
	}

	private String[] readRandomQueries() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				ADAPTIVE_CLASSIFIER_RANDOM_QUERIES_FILE), "UTF8"));
		String q;
		ArrayList<String> queries = new ArrayList<String>();
		while ((q = br.readLine()) != null)
			queries.add(q);
		br.close();
		
		String[] queriesArr = new String[queries.size()];
		for (int i = 0; i < queriesArr.length; i++) {
			queriesArr[i] = queries.get(i);
		}
		return queriesArr;
	}
	public void printConfig() {
		System.out.println("windowsSize: " + windowsSize);
		System.out.println("streamLang: " + streamLang);
		System.out.println("twitterConsumerKey: " + twitterConsumerKey);
		System.out.println("twitterConsumerSecret: " + twitterConsumerSecret);
		System.out.println("twitterAccessToken: " + twitterAccessToken);
		System.out.println("twitterAccessTokenSecret: " + twitterAccessTokenSecret);
		System.out.println("Queries: ");
		for (String q : topicQueries)
			System.out.println("\t" + q);
	}
	
	/**
	 * Setup list of configurations to be used for each crawler thread
	 */
	private void setupCrawlersConfigurations() {

		crawlersConfigurationList = new ArrayList<Configuration>();

		// Ensure and raise an exception if the number of tokens is not equal to
		// the number of proxy servers, except when crawling on one machine with
		// no proxy using only one token
		if (twitterAuthenticationTokens.size() == proxyServersList.size()
				|| (twitterAuthenticationTokens.size() == 1 && proxyServersList.size() == 0)) {
			// Loop on all access tokens and assign corresponding IP
			// Each configuration object will be used to to configure a crawler
			// in a
			// seperate thread
			for (int i = 0; i < twitterAuthenticationTokens.size(); i++) {
				ProxyServer objProxyServer = null;
				if (proxyServersList.size() > 0)
					objProxyServer = proxyServersList.get(i);

				OAuth objOAuth = twitterAuthenticationTokens.get(i);

				ConfigurationBuilder objConfigurationBuilder = new ConfigurationBuilder();
				objConfigurationBuilder.setDebugEnabled(true).setOAuthConsumerKey(objOAuth.getConsumerKey())
						.setOAuthConsumerSecret(objOAuth.getConsumerSecret())
						.setOAuthAccessToken(objOAuth.getAccessToken())
						.setOAuthAccessTokenSecret(objOAuth.getAccessTokenSecret());

				if (objProxyServer != null) {
					objConfigurationBuilder.setHttpProxyHost(objProxyServer.getHost()).setHttpProxyPort(
							objProxyServer.getPort());
				}

				crawlersConfigurationList.add(objConfigurationBuilder.build());
			}
		} else {
			throw new UnsupportedOperationException(
					"Number of access tokens should be equal to number of proxy servers, unless using one "
							+ "access token on single machine with no proxy");
		}
	}

	/**
	 * Load Twitter access token from the corresponding configuration file
	 * 
	 * @throws IOException
	 */
	private void loadTwitterTokens() throws IOException {

		twitterAuthenticationTokens = new ArrayList<OAuth>();
		ArrayList<String> lstData = loadResourceFile(TWITTER_TOKENS_CONFIG_FILE);

		for (String strDataItem : lstData) {
			OAuth objOAuth = new OAuth();
			List<String> lstDataItems = splitStringOnSpace(strDataItem);

			// Column 0 is containing the email this token was created from
			objOAuth.setConsumerKey(lstDataItems.get(1));
			objOAuth.setConsumerSecret(lstDataItems.get(2));
			objOAuth.setAccessToken(lstDataItems.get(3));
			objOAuth.setAccessTokenSecret(lstDataItems.get(4));

			twitterAuthenticationTokens.add(objOAuth);
		}
	}

	/**
	 * Load proxies list from the corresponding configuration file
	 * 
	 * @throws IOException
	 */
	private void loadProxies() throws IOException {

		proxyServersList = new ArrayList<ProxyServer>();
		ArrayList<String> lstData = loadResourceFile(PROXIES_CONFIG_FILE);

		for (String strDataItem : lstData) {
			ProxyServer objProxyServer = new ProxyServer();
			List<String> lstDataItems = splitStringOnSpace(strDataItem);

			objProxyServer.setHost(lstDataItems.get(0));
			objProxyServer.setPort(Integer.parseInt(lstDataItems.get(1)));

			proxyServersList.add(objProxyServer);
		}
	}
	/**
	 * Split input string on one or more spaces
	 * 
	 * @param strDataItem
	 *            input string
	 * @return List of token in input string
	 */
	private List<String> splitStringOnSpace(String strDataItem) {
		return Arrays.asList(strDataItem.trim().split("\\s+"));
	}
	
	/**
	 * Load resource file in array of string, each new line in a separate string
	 * 
	 * @param strResourceFileName
	 *            Resource file name
	 * @return list of strings, containing the data in the input file, each new
	 *         line in a separate string
	 * @throws IOException
	 */
	private ArrayList<String> loadResourceFile(String strResourceFileName) throws IOException {
		ArrayList<String> lstResourceFileData = new ArrayList<String>();
		BufferedReader objBufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(strResourceFileName)));

		String strLine = null;
		while ((strLine = objBufferedReader.readLine()) != null) {
			if (strLine.trim().length() > 0 && !strLine.startsWith("#")) {
				lstResourceFileData.add(strLine);
			}
		}

		objBufferedReader.close();
		return lstResourceFileData;
	}
	
}
