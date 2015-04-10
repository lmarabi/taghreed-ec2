/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.compressors.CompressorException;
import org.eclipse.jdt.core.dom.ThisExpression;
import org.gistic.invertedIndex.KWIndexBuilder;
import org.gistic.invertedIndex.MetaData;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;

import umn.ec2.exp.Initiater;
import umn.ec2.exp.Responder;

/**
 *
 * @author turtle
 */
public class BuildIndex {

	private Commons config;
	private String command;
	private String tweetFile, hashtagFile;
	private static Initiater trigger;
	

	

	public BuildIndex(String tweetFile, String hashtagFile) throws IOException {
		this.config = new Commons();
		this.tweetFile = tweetFile;
		this.hashtagFile = hashtagFile;

	}

	public BuildIndex() throws IOException {
		this.config = new Commons();
	}
	
	public void setTrigger(Responder handler) {
		this.trigger = new Initiater();
		this.trigger.addListener(handler);
	}

	public void setHashtagFile(String hashtagFile) {
		this.hashtagFile = hashtagFile;
	}

	public void setTweetFile(String tweetFile) {
		this.tweetFile = tweetFile;
	}

	/**
	 * This method estimate the selectivity of the MBR
	 * 
	 * @param rtreeFolder
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	// public void AddSelectivityToMasterFile(String rtreeFolder)
	// throws FileNotFoundException, IOException {
	// // Read the master file outputed from spatial hadoop
	// String path = rtreeFolder;
	// BufferedReader reader = new BufferedReader(new FileReader(new File(path
	// + "/_master.quadtree")));
	// List<String> metaData = new ArrayList<String>();
	// String line = null;
	// while ((line = reader.readLine()) != null) {
	// String[] temp = line.split(",");
	// File f = new File(path + "/" + temp[7]);
	// BufferedReader partitionReader = new BufferedReader(new FileReader(
	// f));
	// String tweets = null;
	// int count = 0;
	// while ((tweets = partitionReader.readLine()) != null) {
	// count++;
	// }
	// partitionReader.close();
	// metaData.add(line + "," + count + "\n");
	// }
	// reader.close();
	// // rewrite the master file with the number of data in each partition
	// File f = new File(path + "/_master.quadtree");
	// f.delete();
	// f.createNewFile();
	// FileWriter fileWriter = new FileWriter(f);
	// Iterator it = metaData.iterator();
	// while (it.hasNext()) {
	// fileWriter.write(it.next().toString());
	// }
	// fileWriter.close();
	//
	// }

	/**
	 * This method get the Threshold of MBR in R+tree index
	 * 
	 * @param rtreeFolder
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public int getThreshold(String rtreeFolder) throws FileNotFoundException,
			IOException {
		// Read the master file outputed from spatial hadoop
		String path = rtreeFolder;
		int threshold = Integer.MAX_VALUE;
		BufferedReader reader = new BufferedReader(new FileReader(new File(path
				+ "/_master." + config.getSpatialIndex())));
		List<String> metaData = new ArrayList<String>();
		String line = null;
		int cardinality = 0;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split(",");
			// Get the minimum number of partition
			cardinality = Integer.parseInt(temp[5]);
			if (cardinality > 15000) {
				if (cardinality < threshold) {
					threshold = Integer.parseInt(temp[5]);
				}
			}
		}
		reader.close();

		return threshold < 15000 ? 15000 : threshold;

	}

	/**
	 * This method build the index of one day crawler
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized void CreateRtreeTweetIndex() throws IOException,
			InterruptedException {
		this.UpdatelookupTable(queryLevel.Day, this.tweetFile);
		Thread t = new Thread(new BuildIndexThreads("",queryLevel.Day));
		t.start();
	}

	

	/**
	 * This method create folder in hdfs
	 *
	 * @param folderName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CreateHdfsFolder(String folderName,queryLevel level) throws IOException,
			InterruptedException {
		command = config.getHadoopDir() + "hadoop fs -mkdir "
				+ config.getHadoopHDFSPath() + folderName;
		commandExecuter(command,level);
	}

	/**
	 * This copy file in hdfs under the given folder name
	 *
	 * @param folderName
	 * @param fileDir
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CopytoHdfsFolder(String folderName, String fileDir,queryLevel level)
			throws IOException, InterruptedException {
		String[] fileName = fileDir.split("/");
		command = config.getHadoopDir() + "hadoop distcp " +config.getEc2AccessCode()
				+" "+config.getS3Dir()+ fileName[fileName.length-1]
				+ " " + config.getHadoopHDFSPath() + folderName + "/";
		commandExecuter(command,level);
	}

	/**
	 * This method build the index of the hashtags under the given foldername
	 * And copy the data to the local disk
	 *
	 * @param folderName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void BuildHashtagHdfsIndex(String folderName, String level)
			throws IOException, InterruptedException {
		File f = new File(config.getQueryRtreeIndex() + "hashtags/" + level
				+ "/");
		if (!f.exists()) {
			f.mkdirs();
		}
		// Build index
		command = config.getHadoopDir()
				+ "hadoop jar "
				// + config.getHadoopDir()
				// + "/"
				+ config.getShadoopJar()
				+ " partition -D dfs.block.size="
				+ (128 * 1024 * 1024)
				+ " "
				+ "-libjars "
				// + config.getHadoopDir()
				// + "/"
				+ config.getLibJars() + " " + config.getHadoopHDFSPath()
				+ folderName + " " + config.getHadoopHDFSPath() + "index."
				+ folderName + " -overwrite  sindex:"
				+ config.getSpatialIndex() + " "
				+ "shape:org.gistic.taghreed.spatialHadoop.HashTags"
				+ "  -no-local";
		commandExecuter(command,queryLevel.valueOf(level));
		// Copy to local
		command = config.getHadoopDir() + "hadoop fs -copyToLocal "
				+ config.getHadoopHDFSPath() + "index." + folderName + " "
				+ config.getQueryRtreeIndex() + "hashtags/" + level + "/"
				+ "index." + folderName + "/";

		commandExecuter(command,queryLevel.valueOf(level));
		// remove from hdfs
		command = config.getHadoopDir() + "hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + folderName;

		commandExecuter(command,queryLevel.valueOf(level));
		command = config.getHadoopDir() + "hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + "index." + folderName;

		commandExecuter(command,queryLevel.valueOf(level));

	}

	/**
	 * This method build the index of tweets under the given folderName And then
	 * copy the index to local disk, Folder index must exist in the HDFS
	 *
	 * @param folderName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void BuildTweetHdfsIndex(String folderName, String level)
			throws IOException, InterruptedException {
		this.UpdatelookupTable(queryLevel.valueOf(level), folderName);
		Thread t = new Thread(new BuildIndexThreads(folderName,queryLevel.valueOf(level)));
		t.start();
		
	}

	/**
	 * Level [Day,Week,Month] this method will update the tweets and hashtag
	 * Lookup tables.
	 *
	 * @param level
	 * @throws IOException
	 */
	public void UpdatelookupTable(queryLevel level,String IndexName) throws IOException {
		UpdatelookupTable("tweets", level, config.getQueryRtreeIndex(),IndexName);
		// UpdatelookupTable("hashtags", level, config.getQueryRtreeIndex());
		// UpdatelookupTable("tweets", level, config.getQueryInvertedIndex());
		// UpdatelookupTable("hashtags", level, config.getQueryInvertedIndex());

	}

	private void UpdatelookupTable(String type, queryLevel level, String directory,String IndexName)
			throws IOException {
		
		System.out.println("Update lookupTable Type:" + type + " level:"
				+ level.toString());
		File indexFile = new File(IndexName);
		File lookupTweet = new File(directory + "/" + type + "/"
				+ level.toString() + "/lookupTable.txt");
		File dir = new File(lookupTweet.getParent());
		if (!dir.exists()) {
			dir.mkdirs();
		}
		lookupTweet.createNewFile();
		OutputStreamWriter writer = new FileWriter(lookupTweet, true);
		File lookupDir = new File(directory + "/" + type + "/"
				+ level.toString() + "/");
		writer.write(indexFile.getName().replace(".bz2", "") + "\n");
		writer.close();
	}

	public void createInvertedTweetIndex() throws IOException,
			CompressorException {
		List<File> file = new ArrayList<File>();
		File tweetFolder = new File(config.getQueryInvertedIndex()
				+ "/tweets/Day/");
		if (!tweetFolder.exists()) {
			tweetFolder.mkdirs();
		}
		File tweetsFile = new File(tweetFile);
		file.add(tweetsFile);
		// Create the inverted index
		if (new File(config.getQueryInvertedIndex() + "/tweets/Day/index."
				+ tweetsFile.getName().replaceAll(".bz2", "")).exists()) {
			return;
		}
		KWIndexBuilder indexbuilder = new KWIndexBuilder();
		String indexfolder = config.getQueryInvertedIndex()
				+ "/tweets/Day/index."
				+ tweetsFile.getName().replaceAll(".bz2", "");
		indexbuilder.buildIndex(file, indexfolder,
				KWIndexBuilder.dataType.tweets);
		MetaData md = new MetaData();
		// create the meta data for the index
		md.buildMetaData(config.getQueryInvertedIndex() + "/tweets/Day/index."
				+ tweetsFile.getName().replaceAll(".bz2", ""), config
				.getQueryInvertedIndex(),
				tweetsFile.getName().replaceAll(".bz2", ""),
				getThreshold(config.getQueryRtreeIndex() + "/tweets/Day/index."
						+ tweetsFile.getName().replaceAll(".bz2", "")));
	}

	public void createInvertedHashtagIndex() throws CompressorException {
		List<File> file = new ArrayList<File>();
		File tweetFolder = new File(config.getQueryInvertedIndex()
				+ "/hashtags/Day/");
		if (!tweetFolder.exists()) {
			tweetFolder.mkdirs();
		}
		File tweetFile = new File(hashtagFile);
		file.add(tweetFile);
		// Create the inverted index
		if (new File(config.getQueryInvertedIndex() + "/hashtags/Day/index."
				+ tweetFile.getName()).exists()) {
			return;
		}
		KWIndexBuilder indexbuilder = new KWIndexBuilder();
		String indexfolder = config.getQueryInvertedIndex()
				+ "/hashtags/Day/index." + tweetFile.getName();
		indexbuilder.buildIndex(file, indexfolder,
				KWIndexBuilder.dataType.hashtags);
	}

	

	public static void commandExecuter(String command,queryLevel level) throws IOException,
			InterruptedException {
		System.out.println(command);
		Process myProcess = Runtime.getRuntime().exec(command);

		StreamGobbler errorGobbler = new StreamGobbler(
				myProcess.getErrorStream(), System.out,level,trigger);

		// Any output?
		StreamGobbler outputGobbler = new StreamGobbler(
				myProcess.getInputStream(), System.err,level,trigger);

		errorGobbler.start();
		outputGobbler.start();

		// Any error
		int exitVal = myProcess.waitFor();
		errorGobbler.join(); // Handle condition where the
		outputGobbler.join(); // process ends before the threads finish
	}
	
	

	public static void main(String[] args) throws IOException,
			InterruptedException {
		// String config.getHadoopDir() =
		// "/home/turtle/workspace/hadoop-1.2.1/";
		// String tweetFile =
		// "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/tweets/2013-10-12";
		// String hashtagFile =
		// "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/hashtags/2013-10-12";
		// String config.getQueryRtreeIndex() =
		// "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/result/rtreeindex/";
		// String invertedindex =
		// "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/result/invertedindex/";;
		// BuildIndex index = new BuildIndex(config.getHadoopDir(), tweetFile,
		// hashtagFile, config.getQueryRtreeIndex(), invertedindex);
		// index.UpdatelookupTable(BuildIndex.Level.Day);
		// BuildIndex in = new BuildIndex();
		// in.AddSelectivityToMasterFile("/export/scratch/louai/test/index/rtreeindex/tweets/Day/");
	}

	class BuildIndexThreads implements Runnable {
		String fileName;
		queryLevel level;
		OutputStreamWriter writer;

		public BuildIndexThreads(String fileName,queryLevel level) throws UnsupportedEncodingException, FileNotFoundException {
			this.level = level;
			this.fileName = fileName;
//			this.writer =  new OutputStreamWriter(
//					new FileOutputStream(System.getProperty("user.dir")
//							+ "/indexTime.log", true), "UTF-8");
		}

		@Override
		public void run() {
			try {
				String indexCommand = "index";
				if(config.getSpatialIndex().equals("str") ||
						config.getSpatialIndex().equals("str+") || 
						config.getSpatialIndex().equals("zcurve") ||
						config.getSpatialIndex().equals("kdtree") ||
						config.getSpatialIndex().equals("hilbert") || 
						config.getSpatialIndex().equals("quadtree")){
					indexCommand = "partition";
				}
				if(level.equals(queryLevel.Day)){
					File file = new File(tweetFile);
					File tweetFolder = new File(config.getQueryRtreeIndex() + "tweets/Day/");
					if (!tweetFolder.exists()) {
						tweetFolder.mkdirs();
					}
					if (new File(config.getQueryRtreeIndex() + "tweets/Day/index."
							+ file.getName().replace(".bz2", "")).exists()) {
						return;
					}
					// copy to hdfs
//					command = config.getHadoopDir() + "hadoop fs -copyFromLocal "
//							+ tweetFile + " " + config.getHadoopHDFSPath();
					command = config.getHadoopDir() + "hadoop distcp " +config.getEc2AccessCode()
							+" "+config.getS3Dir()+ file.getName()
							+ " " + config.getHadoopHDFSPath();
					commandExecuter(command,level);
					// Build index
					command = config.getHadoopDir()
							+ "hadoop jar "
							// + config.getHadoopDir()
							// + "/"
							+ config.getShadoopJar()
							+ " "+indexCommand+" "
							+ config.getEc2AccessCode()
							+ " -D dfs.block.size="
							+ (128 * 1024 * 1024)
							+ " "
							+ "-libjars "
							// + config.getHadoopDir()
							// + "/"
							+ config.getLibJars() + " " + config.getS3Dir()
							+ file.getName() + " " + config.getHadoopHDFSPath() + "Day/index."
							+ file.getName().replace(".bz2", "") + " -overwrite  sindex:"
							+ config.getSpatialIndex() + " shape:"
							+ "org.gistic.taghreed.spatialHadoop.Tweets" + "  -no-local";

					long starttime = System.currentTimeMillis();
					commandExecuter(command,level);
					long endtime = System.currentTimeMillis() - starttime;
					synchronized(this){
						this.writer = new OutputStreamWriter(
								new FileOutputStream(
										System.getProperty("user.dir")
												+ "/indexTime.log", true),
								"UTF-8");
						writer.write("\n" + file.getName() + "," + endtime);
						writer.close();
					}
//					// Copy to local
//					command = config.getHadoopDir() + "hadoop fs -copyToLocal "
//							+ config.getHadoopHDFSPath() + "index."
//							+ file.getName().replace(".bz2", "") + " "
//							+ config.getQueryRtreeIndex() + "tweets/Day/" + "index."
//							+ file.getName().replace(".bz2", "") + "/";
//
//					commandExecuter(command);
//					// remove from hdfs
//					command = config.getHadoopDir() + "hadoop fs -rmr "
//							+ config.getHadoopHDFSPath() + file.getName();
//
//					commandExecuter(command);
//					command = config.getHadoopDir() + "hadoop fs -rmr "
//							+ config.getHadoopHDFSPath() + "index."
//							+ file.getName().replace(".bz2", "");
//					commandExecuter(command);
				}else{
					File f = new File(config.getQueryRtreeIndex() + "tweets/" + level + "/");
					if (!f.exists()) {
						f.mkdirs();
					}
					// Build index

					command = config.getHadoopDir()
							+ "hadoop jar "
							// + config.getHadoopDir()
							// + "/"
							+ config.getShadoopJar()
							+ " "+indexCommand+" "
							+ config.getEc2AccessCode()
							+ " -D dfs.block.size="
							+ (128 * 1024 * 1024)
							+ " "
							+ "-libjars "
							// + config.getHadoopDir()
							// + "/"
							+ config.getLibJars() + " " + config.getS3Dir()
							+ this.fileName + " " + config.getHadoopHDFSPath() + level+"/index."
							+ this.fileName + " -overwrite  sindex:"
							+ config.getSpatialIndex() + " shape:"
							+ "org.gistic.taghreed.spatialHadoop.Tweets" + "  -no-local";
					long starttime = System.currentTimeMillis();
					commandExecuter(command,level);
					long endtime = System.currentTimeMillis() - starttime;
					synchronized(this){
						this.writer = new OutputStreamWriter(
								new FileOutputStream(
										System.getProperty("user.dir")
												+ "/indexTime.log", true),
								"UTF-8");
						writer.write("\n" + this.fileName + "," + endtime);
						writer.close();
					}
//						// Copy to local
//						command = config.getHadoopDir() + "hadoop fs -copyToLocal "
//								+ config.getHadoopHDFSPath() + "index." + this.fileName + " "
//								+ config.getQueryRtreeIndex() + "tweets/" + level + "/"
//								+ "index." + this.fileName + " ";
//
//						commandExecuter(command);
//						// remove from hdfs
//						command = config.getHadoopDir() + "hadoop fs -rmr "
//								+ config.getHadoopHDFSPath() + this.fileName;
//
//						commandExecuter(command);
//						command = config.getHadoopDir() + "hadoop fs -rmr "
//								+ config.getHadoopHDFSPath() + "index." + this.fileName;
//
//						commandExecuter(command);
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

}

class StreamGobbler extends Thread {
	InputStream is;
	PrintStream os;
	OutputStreamWriter writer;
	Initiater trigger;

	StreamGobbler(InputStream is, PrintStream os,queryLevel level,Initiater trigger) throws UnsupportedEncodingException, FileNotFoundException {
		this.is = is;
		this.os = os;
		this.writer = new OutputStreamWriter(
				new FileOutputStream(
						System.getProperty("user.dir")
								+ "/IndexTime_"+level.toString()+".log", true),
				"UTF-8");
		this.trigger = trigger;
		
	}

	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is));
			 String line = null;
			 while ((line = reader.readLine()) != null) {
				 os.println(line);
				 writer.write(line+"\n");
				 writer.flush();
				 if(line.matches("Total indexing time in millis \\d+")){
					 trigger.notifyExecutionTime(line);
				 }
			 }
			 writer.close();
		}catch (IOException ioe) {
			//handel error.
		}
		
	}
}




