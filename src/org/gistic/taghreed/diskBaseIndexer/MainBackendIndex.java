/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.compress.compressors.CompressorException;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.gistic.taghreed.Commons;

import umn.ec2.exp.Initiater;
import umn.ec2.exp.Responder;

/**
 *
 * @author turtle
 */
public class MainBackendIndex {

	private String tweetsFile, hashtagsFile;
	private static Commons config;
	private static BuildIndex indexer;
	private BuildPyramidIndex pyramidIndexer;
	private static Responder handler;

	public MainBackendIndex(String tweetsFile, String hashtagsFile)
			throws IOException {
		this.config = new Commons();
		this.tweetsFile = tweetsFile;
		this.hashtagsFile = hashtagsFile;
		this.indexer = new BuildIndex(tweetsFile, hashtagsFile);
		this.pyramidIndexer = new BuildPyramidIndex();
	}

	public MainBackendIndex() throws IOException {
		this.config = new Commons();
		this.pyramidIndexer = new BuildPyramidIndex();
		this.indexer = new BuildIndex();
	}

	public void setTweetsFile(String tweetsFile) throws IOException {
		this.tweetsFile = tweetsFile;
		this.indexer = new BuildIndex();
		this.indexer.setTweetFile(this.tweetsFile);
	}
	
	public void setHandler(Responder handler) {
		this.handler = handler;
	}
	
	
	/***
	 * Invoke this methods by -index day
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void indexDayLevel() throws IOException, InterruptedException{
		List<Thread> threads = new ArrayList<>();
		config = new Commons();
		System.out.println(config.getTweetFlushDir());
		File tweetsFile = new File(config.getTweetFlushDir());
		System.out.println(tweetsFile.getAbsolutePath());
		List<String> sortedtweetsFile;
		sortedtweetsFile = new ArrayList<String>();
		for (String file : tweetsFile.list()) {
			if (!file.equals(".DS_Store")
					&& !file.equals("inputDataStatistics.txt")
					&& !file.equals("inputDataStatistics_Day.cluster")
					&& !file.equals("inputDataStatistics_Week.cluster")
					&& !file.equals("inputDataStatistics_Month.cluster")) {
				sortedtweetsFile.add(config.getTweetFlushDir() + file);
			}
		}
		Collections.sort(sortedtweetsFile);

		indexer.setTrigger(handler);
		System.out.println(sortedtweetsFile.size());
		for (int i = 0; i < sortedtweetsFile.size(); i++) {
			try {
				indexer.setTweetFile(sortedtweetsFile.get(i));
				System.out.println("indexing: "+sortedtweetsFile.get(i));
				threads.add(indexer.CreateRtreeTweetIndex());
			} catch (InterruptedException ex) {
				Logger.getLogger(MainBackendIndex.class.getName()).log(
						Level.SEVERE, null, ex);
			}
		}
		
		for(Thread t : threads){
			t.join();
		}
	}
	
	/***
	 * Invoke this method -index week
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ParseException
	 */
	public static void indexWeekLevel() throws IOException, InterruptedException, ParseException{
		BuildPyramidIndex index = new BuildPyramidIndex();
		index.setHandler(handler);
		index.CreateRtreeTweetWeekIndex();
	}
	
	
	/***
	 * Invoke this -index month
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ParseException
	 */
	public static void indexMonthLevel() throws IOException, InterruptedException, ParseException{
		BuildPyramidIndex index = new BuildPyramidIndex();
		index.setHandler(handler);
		index.createRtreeTweetMonths();
	}
	
	
	public static void indexWholeOneIndex() throws IOException, InterruptedException{
		BuildPyramidIndex index = new BuildPyramidIndex();
		index.setHandler(handler);
		index.createWholeDataIndex();
	}

	public void run(String args[]) throws FileNotFoundException, IOException,
			CompressorException {
		try {
			System.out.println("Enter the run of main backend");
			// Create Index in spatial hadoop
			System.out.println("Build the Day rtree index of tweets*");
			indexer.CreateRtreeTweetIndex();
//			System.out.println("Build the Day inverted index of tweets");
//			 indexer.createInvertedTweetIndex();
			// update lookupTable
			System.out.println("Update the lookup table");
			pyramidIndexer.CreateIndex();
		} catch (FileNotFoundException ex) {
			Logger.getLogger(MainBackendIndex.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (ParseException ex) {
			Logger.getLogger(MainBackendIndex.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(MainBackendIndex.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(MainBackendIndex.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, CompressorException {
		System.out.println("new version 2");
		System.out.println(System.getProperty("user.dir"));
		config = new Commons();
		System.out.println(config.getTweetFlushDir());
		File tweetsFile = new File(config.getTweetFlushDir());
		System.out.println(tweetsFile.getAbsolutePath());
		List<String> sortedtweetsFile;
		sortedtweetsFile = new ArrayList<String>();
		for (String file : tweetsFile.list()) {
			if (!file.equals(".DS_Store")
					&& !file.equals("inputDataStatistics.txt")
					&& !file.equals("inputDataStatistics_Day.cluster")
					&& !file.equals("inputDataStatistics_Week.cluster")
					&& !file.equals("inputDataStatistics_Month.cluster")) {
				sortedtweetsFile.add(config.getTweetFlushDir() + file);
			}
		}
		Collections.sort(sortedtweetsFile);
		// File hashtafolder = new File(config.getHashtagFlushDir());
		// List<String> sortedhashtagFile = new ArrayList<String>();
		// for(String file : hashtafolder.list()){
		// if(!file.equals(".DS_Store")){
		// sortedhashtagFile.add(config.getHashtagFlushDir()+file);
		// }
		// }
		// Collections.sort(sortedhashtagFile);
		// if(sortedhashtagFile.size() != sortedtweetsFile.size()){
		// System.out.println("Erorr hastags and tweet file doesn't match");
		// // return;
		// }
		BuildIndex indexer = new BuildIndex();
		// indexer.CreateRtreeTweetIndex();
		// indexer.CreateRtreeHashtagIndex();
		// indexer.createInvertedHashtagIndex();
		// indexer.createInvertedTweetIndex();
		System.out.println(sortedtweetsFile.size());
		for (int i = 1; i < sortedtweetsFile.size(); i++) {
			//indexer = new BuildIndex();
			indexer.setTweetFile(sortedtweetsFile.get(i));
			try {
				 indexer.CreateRtreeTweetIndex();
//				 index.CreateRtreeHashtagIndex();
//				 index.createInvertedHashtagIndex();
//     		    indexer.createInvertedTweetIndex();
			} catch (InterruptedException ex) {
				Logger.getLogger(MainBackendIndex.class.getName()).log(
						Level.SEVERE, null, ex);
			}
		}
		MainBackendIndex index = new MainBackendIndex();
		index.setTweetsFile(sortedtweetsFile.get(0));
		index.run(args);

	}

}
