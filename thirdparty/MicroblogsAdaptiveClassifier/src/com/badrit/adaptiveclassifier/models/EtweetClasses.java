package com.badrit.adaptiveclassifier.models;

/**
 * TweetBase Classes Enum
 * 
 * @author telganainy
 * 
 */
public enum EtweetClasses {
	/**
	 * Match topic queries
	 */
	MatchTopicQueries,
	/**
	 * Match extended queries
	 */
	MatchExtendedQueries,

	/**
	 * Match SVM classifier
	 */
	RELEVANT,

	/**
	 * Don't match SVM classifier
	 */
	NOT_RELEVANT
}