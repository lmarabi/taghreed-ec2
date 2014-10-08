package com.badrit.adaptiveclassifier.crawler;

public class ProxyServer {

	/**
	 * Proxy server Host/IP
	 */
	String strHost;
	/**
	 * Proxy server Port to connect to
	 */
	Integer intPort;

	public Integer getPort() {
		return intPort;
	}

	public void setPort(Integer intPort) {
		this.intPort = intPort;
	}

	public String getHost() {
		return strHost;
	}

	public void setHost(String strHost) {
		this.strHost = strHost;
	}

	/**
	 * Proxy server constructor
	 */
	public ProxyServer() {
		this.intPort = 0;
		this.strHost = null;
	}

	/**
	 * Proxy server constructor
	 * 
	 * @param strHost
	 *            Proxy server Host/IP
	 * @param intPort
	 *            Proxy server Port to connect to
	 */
	public ProxyServer(String strHost, Integer intPort) {
		this.intPort = intPort;
		this.strHost = strHost;
	}

	/**
	 * Dump Proxy Server object details to a string
	 */
	@Override
	public String toString() {
		String strSerialized = this.strHost + ":" + this.intPort;
		return strSerialized;
	}
}