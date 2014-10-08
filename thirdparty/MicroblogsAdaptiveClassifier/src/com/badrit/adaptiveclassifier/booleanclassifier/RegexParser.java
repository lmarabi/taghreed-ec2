package com.badrit.adaptiveclassifier.booleanclassifier;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author telganainy
 * 
 */
public class RegexParser {

	/**
	 * List of ORed patterns to match input string against
	 */
	List<Pattern> lstPattern = null;

	/**
	 * RegexParser constructor
	 * 
	 * @param strConfiurationFilePath
	 *            Regex configuration file path
	 * @throws IOException
	 */
	public RegexParser(String strConfiurationFilePath) throws IOException {
		lstPattern = new ArrayList<Pattern>();
		loadConfigurationFile(strConfiurationFilePath);
	}

	/**
	 * Load regex file path
	 * 
	 * @param strConfiurationFilePath
	 *            Regex configuration file path
	 * @throws IOException
	 */
	private void loadConfigurationFile(String strConfiurationFilePath) throws IOException {
		BufferedReader objBufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(
				strConfiurationFilePath), "UTF8"));

		String strLine = null;
		while ((strLine = objBufferedReader.readLine()) != null) {
			// Skip comments and empty lines
			if (!strLine.startsWith("##") && strLine.length() > 0)
				lstPattern.add(Pattern.compile(strLine));
		}

		objBufferedReader.close();
	}

	/**
	 * Check if a string matches any of the provided REGEX in the corresponding
	 * configuration file
	 * 
	 * @param strText
	 *            input string
	 * @return true, if input string matches any of the provided REGEX in the
	 *         corresponding configuration file
	 */
	public Boolean matches(String strText) {
		Boolean blnMatch = false;

		// Add extra white space at the end of the input string to match regex
		// with space at the end
		strText += " ";

		for (int i = 0; i < lstPattern.size(); i++) {
			Matcher objMatcher = lstPattern.get(i).matcher(strText);
			if (objMatcher.find()) {
				blnMatch = true;
				break;
			}
		}

		return blnMatch;
	}

	/**
	 * Count of input string matches REGEX from corresponding configuration file
	 * 
	 * @param strText
	 *            input string
	 * @return the count of input string matches REGEX from corresponding
	 *         configuration file
	 */
	public Integer matchesCount(String strText) {
		Integer intMatchesCount = 0;

		// TODO: are we really interested in multiple occurences, wouldn't it be
		// faster, if we returned "true" or "false" ?
		for (int i = 0; i < lstPattern.size(); i++) {
			Matcher objMatcher = lstPattern.get(i).matcher(strText);
			if (objMatcher.find()) {
				intMatchesCount++;
			}
		}

		return intMatchesCount;
	}

}
