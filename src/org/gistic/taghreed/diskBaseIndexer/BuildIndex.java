/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gistic.invertedIndex.KWIndexBuilder;
import org.gistic.invertedIndex.MetaData;
import org.gistic.taghreed.Commons;

/**
 *
 * @author turtle
 */
public class BuildIndex {

	private Commons config;
	private String command;
	private String tweetFile, hashtagFile;

	public enum Level {

		Day, Week, Month;
	}

	public BuildIndex(String tweetFile, String hashtagFile) throws IOException {
		this.config = new Commons();
		this.tweetFile = tweetFile;
		this.hashtagFile = hashtagFile;
	}

	public BuildIndex() throws IOException {
		this.config = new Commons();
	}


	/**
	 * This method estimate the selectivity of the MBR
	 * 
	 * @param rtreeFolder
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void AddSelectivityToMasterFile(String rtreeFolder)
			throws FileNotFoundException, IOException {
		// Read the master file outputed from spatial hadoop
		String path = rtreeFolder;
		BufferedReader reader = new BufferedReader(new FileReader(new File(path
				+ "/_master.str+")));
		List<String> metaData = new ArrayList<String>();
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split(",");
			File f = new File(path + "/" + temp[7]);
			BufferedReader partitionReader = new BufferedReader(new FileReader(
					f));
			String tweets = null;
			int count = 0;
			while ((tweets = partitionReader.readLine()) != null) {
				count++;
			}
			partitionReader.close();
			metaData.add(line + "," + count + "\n");
		}
		reader.close();
		// rewrite the master file with the number of data in each partition
		File f = new File(path + "/_master.str+");
		f.delete();
		f.createNewFile();
		FileWriter fileWriter = new FileWriter(f);
		Iterator it = metaData.iterator();
		while (it.hasNext()) {
			fileWriter.write(it.next().toString());
		}
		fileWriter.close();

	}
	
	
	/**
	 * This method get the Threshold of MBR in R+tree index
	 * 
	 * @param rtreeFolder
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static int getThreshold(String rtreeFolder)
			throws FileNotFoundException, IOException {
		// Read the master file outputed from spatial hadoop
		String path = rtreeFolder;
		int threshold = Integer.MAX_VALUE;
		BufferedReader reader = new BufferedReader(new FileReader(new File(path
				+ "/_master.str+")));
		List<String> metaData = new ArrayList<String>();
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split(",");
			//Get the minimum number of partition
			if(Integer.parseInt(temp[5]) < threshold){
				threshold = Integer.parseInt(temp[5]);
			}
		}
		reader.close();
		return threshold;

	}

	/**
	 * This method build the index of one day crawler
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CreateRtreeTweetIndex() throws IOException,
			InterruptedException {
		File file = new File(tweetFile);
		File tweetFolder = new File(config.getQueryRtreeIndex() + "tweets/Day/");
		if (!tweetFolder.exists()) {
			tweetFolder.mkdirs();
		}
		if (new File(config.getQueryRtreeIndex() + "tweets/Day/index."
				+ file.getName()).exists()) {
			return;
		}
		// copy to hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -copyFromLocal "
				+ tweetFile + " " + config.getHadoopHDFSPath();
		commandExecuter(command);
		// Build index
		command = config.getHadoopDir()
				+ "/bin/hadoop jar "
				+ config.getHadoopDir()
				+ "/"
				+ config.getShadoopJar()
				+ " index "
				+ "-libjars "
				+ config.getHadoopDir()
				+ "/"
				+ config.getLibJars()
				+" "
				+ config.getHadoopHDFSPath()
				+ file.getName()
				+ " "
				+ config.getHadoopHDFSPath()
				+ "index."
				+ file.getName()
				+ " -overwrite  sindex:str+ shape:"
				+ "org.gistic.taghreed.spatialHadoop.Tweets"
				+ " blocksize:12.mb -no-local";

		commandExecuter(command);
		// Copy to local
		command = config.getHadoopDir() + "/bin/hadoop fs -copyToLocal "
				+ config.getHadoopHDFSPath() + "index." + file.getName() + " "
				+ config.getQueryRtreeIndex() + "tweets/Day/"+ "index." + file.getName() + "/";

		commandExecuter(command);
		// remove from hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + file.getName();

		commandExecuter(command);
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + "index." + file.getName();

		commandExecuter(command);
		
	}

	/**
	 * this method build the index of one day crawler
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CreateRtreeHashtagIndex() throws IOException,
			InterruptedException {
		File file = new File(hashtagFile);
		File tweetFolder = new File(config.getQueryRtreeIndex()
				+ "hashtags/Day/");
		if (!tweetFolder.exists()) {
			tweetFolder.mkdirs();
		}
		if (new File(config.getQueryRtreeIndex() + "hashtags/Day/index."
				+ file.getName()).exists()) {
			return;
		}
		// copy to hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -copyFromLocal "
				+ hashtagFile + " " + config.getHadoopHDFSPath();

		commandExecuter(command);
		// Build index
		command = config.getHadoopDir()
				+ "/bin/hadoop jar "
				+ config.getHadoopDir()
				+ "/"
				+ config.getShadoopJar()
				+ " index "
				+ "-libjars "
				+ config.getHadoopDir()
				+ "/"
				+ config.getLibJars()
				+" "
				+ config.getHadoopHDFSPath()
				+ file.getName()
				+ " "
				+ config.getHadoopHDFSPath()
				+ "index."
				+ file.getName()
				+ " -overwrite  sindex:str+ "
				+ "shape:org.gistic.taghreed.spatialHadoop.HashTags "
				+ "blocksize:12.mb -no-local";

		commandExecuter(command);
		// Copy to local
		command = config.getHadoopDir() + "/bin/hadoop fs -copyToLocal "
				+ config.getHadoopHDFSPath() + "index." + file.getName() + " "
				+ config.getQueryRtreeIndex() + "hashtags/Day/"+ "index." + file.getName() + "/";

		commandExecuter(command);
		// remove from hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + file.getName();

		commandExecuter(command);
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + "index." + file.getName();

		commandExecuter(command);

	}

	/**
	 * This method create folder in hdfs
	 *
	 * @param folderName
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CreateHdfsFolder(String folderName) throws IOException,
			InterruptedException {
		command = config.getHadoopDir() + "/bin/hadoop fs -mkdir "
				+ config.getHadoopHDFSPath() + folderName;
		commandExecuter(command);
	}

	/**
	 * This copy file in hdfs under the given folder name
	 *
	 * @param folderName
	 * @param fileDir
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void CopytoHdfsFolder(String folderName, String fileDir)
			throws IOException, InterruptedException {
		command = config.getHadoopDir() + "/bin/hadoop fs -copyFromLocal "
				+ fileDir + " " + config.getHadoopHDFSPath() + folderName
				+ "/";
		commandExecuter(command);
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
				+ "/bin/hadoop jar "
				+ config.getHadoopDir()
				+ "/"
				+ config.getShadoopJar()
				+ " index "
				+ "-libjars "
				+ config.getHadoopDir()
				+ "/"
				+ config.getLibJars()
				+" "
				+ config.getHadoopHDFSPath()
				+ folderName
				+ " "
				+ config.getHadoopHDFSPath()
				+ "index."
				+ folderName
				+ " -overwrite  sindex:str+ "
				+ "shape:org.gistic.taghreed.spatialHadoop.HashTags"
				+ " blocksize:12.mb -no-local";
		commandExecuter(command);
		// Copy to local
		command = config.getHadoopDir() + "/bin/hadoop fs -copyToLocal "
				+ config.getHadoopHDFSPath() + "index." + folderName + " "
				+ config.getQueryRtreeIndex() + "hashtags/" + level + "/"+ "index." + folderName + "/";

		commandExecuter(command);
		// remove from hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + folderName;

		commandExecuter(command);
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + "index." + folderName;

		commandExecuter(command);
		
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
		File f = new File(config.getQueryRtreeIndex() + "tweets/" + level + "/");
		if (!f.exists()) {
			f.mkdirs();
		}
		// Build index
		command = config.getHadoopDir()
				+ "/bin/hadoop jar "
				+ config.getHadoopDir()
				+ "/"
				+ config.getShadoopJar()
				+ " index "
				+ "-libjars "
				+ config.getHadoopDir()
				+ "/"
				+ config.getLibJars()
				+" "
				+ config.getHadoopHDFSPath()
				+ folderName
				+ " "
				+ config.getHadoopHDFSPath()
				+ "index."
				+ folderName
				+ " -overwrite  sindex:str+ shape:"
				+ "org.gistic.taghreed.spatialHadoop.Tweets"
				+ " blocksize:12.mb -no-local";

		commandExecuter(command);
		// Copy to local
		command = config.getHadoopDir() + "/bin/hadoop fs -copyToLocal "
				+ config.getHadoopHDFSPath() + "index." + folderName + " "
				+ config.getQueryRtreeIndex() + "tweets/" + level + "/"+ "index." + folderName + " ";

		commandExecuter(command);
		// remove from hdfs
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + folderName;

		commandExecuter(command);
		command = config.getHadoopDir() + "/bin/hadoop fs -rmr "
				+ config.getHadoopHDFSPath() + "index." + folderName;

		commandExecuter(command);
		
	}

	/**
	 * Level [Day,Week,Month] this method will update the tweets and hashtag
	 * Lookup tables.
	 *
	 * @param level
	 * @throws IOException
	 */
	public void UpdatelookupTable(Level level) throws IOException {
		UpdatelookupTable("tweets", level, config.getQueryRtreeIndex());
//		UpdatelookupTable("hashtags", level, config.getQueryRtreeIndex());
		UpdatelookupTable("tweets", level, config.getQueryInvertedIndex());
//		UpdatelookupTable("hashtags", level, config.getQueryInvertedIndex());

	}

	private void UpdatelookupTable(String type, Level level, String directory)
			throws IOException {
/*
		System.out.println("Update lookupTable Type:" + type + " level:"
				+ level.toString());
		File lookupTweet = new File(directory + "/" + type + "/"
				+ level.toString() + "/lookupTable.txt");
		System.out.println(lookupTweet);
		if (lookupTweet.exists()) {
			lookupTweet.delete();
		}
		OutputStreamWriter writer = new FileWriter(lookupTweet, true);
		File lookupDir = new File(directory + "/" + type + "/"
				+ level.toString() + "/");
		File[] indeces = lookupDir.listFiles();
		for (File f : indeces) {
			if (!f.getName().equals("lookupTable.txt")) {
				String[] fileName;
				fileName = f.getName().split("\\.");
				// writer.write(fileName[1] + ","+directory+ type +"/" +
				// level.toString() + "/" + f.getName() + "\n");
				writer.write(fileName[1] + "\n");

			}
		}
		writer.close();
*/
	}

	public void createInvertedTweetIndex() throws IOException {
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
				+ tweetsFile.getName()).exists()) {
			return;
		}
		KWIndexBuilder indexbuilder = new KWIndexBuilder();
		String indexfolder = config.getQueryInvertedIndex()
				+ "/tweets/Day/index." + tweetsFile.getName();
		indexbuilder.buildIndex(file, indexfolder,
				KWIndexBuilder.dataType.tweets);
		MetaData md = new MetaData();
		// create the meta data for the index
		md.buildMetaData(config.getQueryInvertedIndex() + "/tweets/Day/index."
				+ tweetsFile.getName(), config.getQueryInvertedIndex(),
				tweetsFile.getName(),getThreshold(config.getQueryRtreeIndex()+ "/tweets/Day/index."
						+ tweetsFile.getName()));
	}

	public void createInvertedHashtagIndex() {
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

	public void commandExecuter(String command) throws IOException,
			InterruptedException {
		System.out.println(command);
		Process myProcess = Runtime.getRuntime().exec(command);
		myProcess.waitFor();
		 BufferedReader in = new BufferedReader(
		 new InputStreamReader(myProcess.getInputStream()));
		 String line = null;
		 while ((line = in.readLine()) != null) {
		 System.out.println(line);
		 }
		 in = new BufferedReader(
		 new InputStreamReader(myProcess.getErrorStream()));
		 line = null;
		 while ((line = in.readLine()) != null) {
		 System.out.println(line);
		 }
		 in.close();

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
//		BuildIndex in = new BuildIndex();
//		in.AddSelectivityToMasterFile("/export/scratch/louai/test/index/rtreeindex/tweets/Day/");
	}
}
