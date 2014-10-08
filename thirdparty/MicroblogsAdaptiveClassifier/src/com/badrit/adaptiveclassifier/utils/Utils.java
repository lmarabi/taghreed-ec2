package com.badrit.adaptiveclassifier.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.UserMentionEntity;

import com.badrit.adaptiveclassifier.crawler.ClassifierConfiguration;

public class Utils {

	public final static String CONTROL_CHARACHTERS = "[\u0000-\u001f]";
	public final static String XML_SPECIAL_CHARACHTERS = "[\\\\<>\\[\\]&'\"]";
	public final static String ALL_SPECIAL_CHARACHTERS = "[+.^*:,?!\"'()\\[\\]{}&<>&;/\\\\]";
	public final static String RETWEET = " RT ";
	public final static String RETWEET_BEGINNING = "^RT ";
	public final static String HEART_TEXT = "<3";
	public final static String HEART_EMOTION = "♥";
	public final static String LOL_ENGLISH = "(?i)((^l+| l+)o+(l+ |l+$))";
	public final static String LOL_ARABIC = "\\b(ل)+(و)+(ل)+\\b";
	public final static String HAHA_ARABIC = "\\bه(ه)+\\b";
	public final static String EMOTICONS = "((?::|;|=)(?:-)?(?:\\)|D|P|p|d))";
	public final static String NUMBERS = "[0-9]";

	private static HashSet<String> stopWords;

	public static String parseSource(String source) {
		if (source != null && source.contains(">") && source.contains("</")) {

			source = source.substring(source.indexOf(">") + 1,
					source.indexOf("</")).trim();
		}
		return source;
	}

	public static List<String> parseHashtags(HashtagEntity[] hashtagEntities) {
		if (hashtagEntities.length == 0)
			return null;

		ArrayList<String> lstHashTags = new ArrayList<String>();
		for (int j = 0; j < hashtagEntities.length; j++)
			lstHashTags.add(hashtagEntities[j].getText());

		return lstHashTags;
	}

	public static List<String> parseMentions(
			UserMentionEntity[] userMentionEntities) {

		if (userMentionEntities.length == 0)
			return null;

		List<String> lstMentions = new ArrayList<String>();
		for (int j = 0; j < userMentionEntities.length; j++)
			lstMentions.add(userMentionEntities[j].getScreenName());

		return lstMentions;
	}

	public static String parseCountry(Place place) {
		return (place == null ? null : processTweetTextForStore(place
				.getCountry()));
	}

	public static String parsePlaceFullName(Place place) {
		return (place == null ? null : processTweetTextForStore(place
				.getFullName()));
	}

	public static String parseGeolocation(GeoLocation geoLocation) {
		return (geoLocation == null ? null : String.format("[%.2f/%.2f]",
				geoLocation.getLatitude(), geoLocation.getLongitude()));
	}

	/**
	 * Remove extra white spaces from input string
	 * 
	 * @param strText
	 *            input string containing extra white spaces at the beginning or
	 *            at the end or repeated empty spaces in the middle of the
	 *            string
	 * @return input string after removing extra whitespaces
	 */
	static public String removeExtraWhiteSpaces(String strText) {
		String strNewString = strText;
		strNewString = strNewString.replaceAll(" +", " ");
		strNewString = strNewString.trim();
		return strNewString;
	}

	/**
	 * Process tweet original text not to cause problem while storing in data
	 * store
	 * 
	 * @param strNonProcessedText
	 *            TweetBase Original Text without any processing
	 * @return TweetBase text processed to be stored in data store
	 */
	public static String processTweetTextForStore(String strNonProcessedText) {
		if (strNonProcessedText == null)
			return null;
		String strProcessedText = strNonProcessedText;

		// Remove special charachters that may cause problems while adding text
		// to the data store
		strProcessedText = strProcessedText
				.replaceAll(CONTROL_CHARACHTERS, " ");
		strProcessedText = strProcessedText.replaceAll(XML_SPECIAL_CHARACHTERS,
				" ");
		strProcessedText = strProcessedText.replaceAll(ALL_SPECIAL_CHARACHTERS,
				" ");
		// Remove extra white spaces
		strProcessedText = removeExtraWhiteSpaces(strProcessedText);

		return strProcessedText;
	}

	/**
	 * Process tweet orginal text to be used for indexing (remove special
	 * charachters, URLs, normalization, etc...)
	 * 
	 * @param strNonProcessedText
	 *            TweetBase Original Text without any processing
	 * @return TweetBase text after processing to be used for indexing
	 * @throws java.io.IOException
	 */
	public static String processTweetTextForIndex(String strNonProcessedText)
			throws IOException {
		String strProcessedText = strNonProcessedText;

		if (stopWords == null) {
			stopWords = new HashSet<String>();
			readStopWords();
		}
		strProcessedText = removeStopWords(strProcessedText);
		strProcessedText = removeMentions(strProcessedText);

		// Remove URLs and any extra not needed information
		strProcessedText = removeURLs(strProcessedText);

		// Remove special charachters
		strProcessedText = strProcessedText
				.replaceAll(CONTROL_CHARACHTERS, " ");
		strProcessedText = strProcessedText.replaceAll(EMOTICONS, " ");
		strProcessedText = strProcessedText.replace(HEART_TEXT, " ");
		strProcessedText = strProcessedText.replaceAll(XML_SPECIAL_CHARACHTERS,
				" ");
		strProcessedText = strProcessedText.replaceAll(ALL_SPECIAL_CHARACHTERS,
				" ");
		strProcessedText = strProcessedText.replaceAll(RETWEET, " ");
		strProcessedText = strProcessedText.replaceAll(RETWEET_BEGINNING, " ");

		// Normalize text
		strProcessedText = strProcessedText.replaceAll(LOL_ENGLISH, " ");
		strProcessedText = strProcessedText.replaceAll(LOL_ARABIC, " ");
		strProcessedText = strProcessedText.replaceAll(HAHA_ARABIC, " ");
		strProcessedText = strProcessedText.toLowerCase();
		strProcessedText = strProcessedText.replaceAll(NUMBERS, " ");

		// Remove extra white spaces
		strProcessedText = Utils.removeExtraWhiteSpaces(strProcessedText);

		return strProcessedText;
	}

	private static String removeMentions(String tweetText) {
		String[] tokenizedTweetText = tweetText.split(" +");
		String tweetTextProcessed = "";

		for (String word : tokenizedTweetText) {
			if (word.trim().length() == 0)
				continue;
			if (word.trim().startsWith("@"))
				continue;
			tweetTextProcessed += " " + word;
		}
		return tweetTextProcessed.trim();
	}

	private static String removeStopWords(String tweetText) {
		String[] tokenizedTweetText = tweetText.split(" +");
		String tweetTextProcessed = "";

		for (String word : tokenizedTweetText) {
			if (word.trim().length() == 0)
				continue;
			// check stop word
			if (stopWords.contains(word))
				continue;
			tweetTextProcessed += " " + word;
		}
		return tweetTextProcessed.trim();
	}

	private static void readStopWords() throws IOException {
		String lang = ClassifierConfiguration.getInstance().streamLang;

		BufferedReader inTrack = new BufferedReader(new InputStreamReader(
				new FileInputStream("resources//lang//stopwords_" + lang
						+ ".txt"), "UTF8"));

		String l = null;
		while ((l = inTrack.readLine()) != null) {
			if (l.startsWith("#") || l.length() == 0)
				continue;
			stopWords.add(l.trim());
		}
		inTrack.close();
	}

	static public String removeURLs(String strOriginal) {
		String strNew = strOriginal;
		String strURLPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern objPattern = Pattern.compile(strURLPattern,
				Pattern.CASE_INSENSITIVE);
		Matcher objMatcher = objPattern.matcher(strNew);
		int i = 0;
		while (objMatcher.find()) {
			String match = "";
			try {
				match = objMatcher.group(i);
				if (match.length() > 0 && match.endsWith(")")) {
					match = match.substring(0, match.length() - 1);
				}
				strNew = strNew.replaceAll(match, " ");
			} catch (Exception e) {
				System.err.println("Failed to remove URL:" + strNew);
				System.err.println("match:" + match);
			}
			i++;
		}

		removeExtraWhiteSpaces(strNew);

		return strNew;
	}
}
