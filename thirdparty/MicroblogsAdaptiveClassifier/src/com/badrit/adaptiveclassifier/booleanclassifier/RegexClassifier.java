package com.badrit.adaptiveclassifier.booleanclassifier;

import java.io.IOException;

/**
 * @author telganainy
 * 
 */
public class RegexClassifier {

	private RegexParser objRegexParser = null;

	public RegexClassifier(String strConfiurationFilePath) throws IOException {
		objRegexParser = new RegexParser(strConfiurationFilePath);
	}

	public Boolean classify(String strProcessedTweetText) {
		return objRegexParser.matches(strProcessedTweetText);
	}
}