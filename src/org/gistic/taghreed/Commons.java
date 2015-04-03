package org.gistic.taghreed;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by saifalharthi on 5/29/14.
 */
public class Commons {

    public static int portNumber;

    public static String tweetFlushDir;
    
    public static String hashtagFlushDir;

    public static String queryRtreeIndex;
    
    public static String queryInvertedIndex;
    
    public static String hadoopDir;
    
    public static String hadoopHDFSPath;
    
    public static String accessToken;
    
    public static String consumerSecret;
    
    public static String consumerKey;
    
    public static String accessTokenSecret;
    
    public static String filterLocationsFile;
    
    public static String shadoopJar;
    
    public static String libJars;
    
    public static String spatialIndex;
    
    public static String ec2AccessCode;
    
    public static String S3Dir;
    

    public Commons() throws IOException {
        this.loadConfigFile();
       
    }
    
    public static String getHadoopHDFSPath() {
		return hadoopHDFSPath;
	}
    
    public static void setHadoopHDFSPath(String hadoopHDFSPath) {
		Commons.hadoopHDFSPath = hadoopHDFSPath;
	}
    

    public  int getPortNumber() {
        return portNumber;
    }

    public  void setPortNumber(int portNumber) {
        Commons.portNumber = portNumber;
    }

    public  String getTweetFlushDir() {
        return tweetFlushDir;
    }

    public void setTweetFlushDir(String tweetFlushDir) {
        Commons.tweetFlushDir = tweetFlushDir;
    }

    public  String getHashtagFlushDir() {
        return hashtagFlushDir;
    }

    public  void setHashtagFlushDir(String hashtagFlushDir) {
        Commons.hashtagFlushDir = hashtagFlushDir;
    }

    public  String getQueryRtreeIndex() {
        return queryRtreeIndex;
    }

    public  void setQueryRtreeIndex(String queryRtreeIndex) {
        Commons.queryRtreeIndex = queryRtreeIndex;
    }

    public  String getQueryInvertedIndex() {
        return queryInvertedIndex;
    }

    public  void setQueryInvertedIndex(String queryInvertedIndex) {
        Commons.queryInvertedIndex = queryInvertedIndex;
    }

    public  String getHadoopDir() {
        return hadoopDir;
    }

    public  void setHadoopDir(String hadoopDir) {
        Commons.hadoopDir = hadoopDir;
    }

    public  String getAccessToken() {
        return accessToken;
    }
    public  void setAccessToken(String accessToken) {
        Commons.accessToken = accessToken;
    }

    public  String getConsumerSecret() {
        return consumerSecret;
    }

    public  void setConsumerSecret(String consumerSecret) {
        Commons.consumerSecret = consumerSecret;
    }

    public  String getConsumerKey() {
        return consumerKey;
    }

    public  void setConsumerKey(String consumerKey) {
        Commons.consumerKey = consumerKey;
    }

    public  String getAccessTokenSecret() {
        return accessTokenSecret;
    }

    public  void setAccessTokenSecret(String accessTokenSecret) {
        Commons.accessTokenSecret = accessTokenSecret;
    }

    public  String getFilterLocationsFile() {
        return filterLocationsFile;
    }

    public  void setFilterLocationsFile(String filterLocationsFile) {
        Commons.filterLocationsFile = filterLocationsFile;
    }
    
    public static String getShadoopJar() {
		return shadoopJar;
	}
    
    public static void setShadoopJar(String shadoopJar) {
		Commons.shadoopJar = shadoopJar;
	}
    
    public static String getLibJars() {
		return libJars;
	}
    
    public static void setLibJars(String libJars) {
		Commons.libJars = libJars;
	}
    
    public static String getSpatialIndex() {
		return spatialIndex;
	}
    
    public static String getEc2AccessCode() {
		return ec2AccessCode;
	}
    
    public static String getS3Dir() {
		return S3Dir;
	}
    
    
    private void loadConfigFile() throws IOException {

        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));
//      prop.load(new FileInputStream("config_testOnly.properties"));
        Commons.accessToken = prop.getProperty("accessToken");
        Commons.accessTokenSecret = prop.getProperty("accessTokenSecret");
        Commons.consumerKey = prop.getProperty("consumerKey");
        Commons.consumerSecret = prop.getProperty("consumerSecret");
        Commons.filterLocationsFile = prop.getProperty("filterLocationsFile");
        Commons.hadoopDir = prop.getProperty("hadoopDir");
        Commons.shadoopJar = prop.getProperty("shadoopJar");
        Commons.libJars = prop.getProperty("libJars");
        Commons.hadoopHDFSPath = prop.getProperty("hadoopHDFSPath");
        Commons.hashtagFlushDir = prop.getProperty("hashtagFlushDir");
        Commons.portNumber = Integer.parseInt(prop.getProperty("portNumber"));
        Commons.queryInvertedIndex = prop.getProperty("queryInvertedIndex");
        Commons.queryRtreeIndex = prop.getProperty("queryRtreeIndex");
        Commons.tweetFlushDir = prop.getProperty("tweetFlushDir");
        Commons.spatialIndex = prop.getProperty("spatialIndex");
        Commons.ec2AccessCode = prop.getProperty("ec2AccessCode");
        Commons.S3Dir = prop.getProperty("S3Dir");
        System.out.println("Config file Loaded");


    }


}
