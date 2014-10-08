package com.badrit.adaptiveclassifier.crawler;

public class OAuth {
	/**
	 * Consumer KEy
	 */
	private String strConsumerKey;
	/**
	 * Consumer Secret
	 */
	private String strConsumerSecret;
	/**
	 * Access Token
	 */
	private String strAccessToken;
	/**
	 * Access Token Secret
	 */
	private String strAccessTokenSecret;

	public String getConsumerKey() {
		return strConsumerKey;
	}

	public void setConsumerKey(String strConsumerKey) {
		this.strConsumerKey = strConsumerKey;
	}

	public String getConsumerSecret() {
		return strConsumerSecret;
	}

	public void setConsumerSecret(String strConsumerSecret) {
		this.strConsumerSecret = strConsumerSecret;
	}

	public String getAccessToken() {
		return strAccessToken;
	}

	public void setAccessToken(String strAccessToken) {
		this.strAccessToken = strAccessToken;
	}

	public String getAccessTokenSecret() {
		return strAccessTokenSecret;
	}

	public void setAccessTokenSecret(String strAccessTokenSecret) {
		this.strAccessTokenSecret = strAccessTokenSecret;
	}

	/**
	 * OAuth Constructor
	 */
	public OAuth() {
		this.strConsumerKey = null;
		this.strConsumerSecret = null;
		this.strAccessToken = null;
		this.strAccessTokenSecret = null;
	}

	/**
	 * OAuth Constructor
	 * 
	 * @param strConsumerKey
	 *            Consumer Key
	 * @param strConsumerSecret
	 *            Consumer Seecret
	 * @param strAccessToken
	 *            Access Token
	 * @param strAccessTokenSecret
	 *            Access Token Secret
	 */
	public OAuth(String strConsumerKey, String strConsumerSecret, String strAccessToken, String strAccessTokenSecret) {
		this.strConsumerKey = strConsumerKey;
		this.strConsumerSecret = strConsumerSecret;
		this.strAccessToken = strAccessToken;
		this.strAccessTokenSecret = strAccessTokenSecret;
	}

	/**
	 * Dump OAuth object to string
	 */
	@Override
	public String toString() {
		String strSerialized = this.strConsumerKey + "\t" + this.strConsumerSecret + "\t" + this.strAccessToken + "\t"
				+ this.strAccessTokenSecret;
		return strSerialized;
	}
}
