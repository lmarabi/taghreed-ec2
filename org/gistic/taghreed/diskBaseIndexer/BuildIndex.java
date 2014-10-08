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

    private String hadoopDir;
    private String tweetsFile;
    private String hashtagsFile;
    private String indexDir;
    private String command;
    private String invertedIndex;

    public enum Level {

        Day, Week, Month;
    }

    public BuildIndex(String tweetFile, String hashtagFile) throws IOException {
        Commons config = new Commons();
        this.invertedIndex = config.getQueryInvertedIndex();
        this.hadoopDir = config.getHadoopDir();
        this.indexDir = config.getQueryRtreeIndex();
        this.tweetsFile = tweetFile;
        this.hashtagsFile = hashtagFile;
    }
    
    public BuildIndex() throws IOException{
        Commons config = new Commons();
        this.invertedIndex = config.getQueryInvertedIndex();
        this.hadoopDir = config.getHadoopDir();
        this.indexDir = config.getQueryRtreeIndex();
    }

    public BuildIndex(String hadoopDir, String tweetsFile, String hashtagsFile, String indexDir, String invertedIndex) {
        this.hadoopDir = hadoopDir;
        this.tweetsFile = tweetsFile;
        this.hashtagsFile = hashtagsFile;
        this.indexDir = indexDir;
        this.invertedIndex = invertedIndex;
    }
    
    
    public void AddSelectivityToMasterFile(String rtreeFolder) throws FileNotFoundException, IOException{
        //Read the master file outputed from spatial hadoop 
        String path = rtreeFolder;
        BufferedReader reader = new BufferedReader(
                new FileReader(new File(path+"/_master.r+tree")));
        List<String> metaData = new ArrayList<String>();
        String line = null;
        while((line = reader.readLine()) != null){
            String[] temp = line.split(",");
            File f = new File(path+"/"+temp[5]);
            BufferedReader partitionReader = new BufferedReader(new FileReader(f));
            String tweets = null;
            int count = 0;
            while((tweets = partitionReader.readLine()) != null){
                count++;
            }
            partitionReader.close();
            metaData.add(line+","+count+"\n");
        }
        reader.close();
        // rewrite the master file with the number of data in each partition
        File f = new File(path+"/_master.r+tree");
        f.delete();
        f.createNewFile();
        FileWriter fileWriter = new FileWriter(f);
        Iterator it = metaData.iterator();
        while(it.hasNext()){
            fileWriter.write(it.next().toString());
        }
        fileWriter.close();
        
    }

    /**
     * This method build the index of one day crawler
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void CreateRtreeTweetIndex() throws IOException, InterruptedException {
        File file = new File(tweetsFile);
        File tweetFolder = new File(indexDir + "tweets/Day/");
        if (!tweetFolder.exists()) {
            tweetFolder.mkdirs();
        }
        if (new File(indexDir + "tweets/Day/index." + file.getName()).exists()) {
            return;
        }
        //copy to hdfs 
        command = hadoopDir + "bin/hadoop fs -copyFromLocal " + tweetsFile + " /";
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Build index 
        command = hadoopDir + "bin/shadoop index /" + file.getName() + " /index." + file.getName() + " -overwrite  sindex:str+ shape:tweets blocksize:12.mb -no-local";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Copy to local
        command = hadoopDir + "bin/hadoop fs -copyToLocal" + " /index." + file.getName() + " " + indexDir + "tweets/Day/";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //remove from hdfs 
        command = hadoopDir + "bin/hadoop fs -rmr" + " /" + file.getName();
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        command = hadoopDir + "bin/hadoop fs -rmr" + " /index." + file.getName();
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        
        AddSelectivityToMasterFile(indexDir + "tweets/Day/"+" /index." + file.getName());
    }

    /**
     * this method build the index of one day crawler
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void CreateRtreeHashtagIndex() throws IOException, InterruptedException {
        File file = new File(hashtagsFile);
        File tweetFolder = new File(indexDir + "hashtags/Day/");
        if (!tweetFolder.exists()) {
            tweetFolder.mkdirs();
        }
        if (new File(indexDir + "hashtags/Day/index." + file.getName()).exists()) {
            return;
        }
        //copy to hdfs 
        command = hadoopDir + "bin/hadoop fs -copyFromLocal " + hashtagsFile + " /";
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Build index 
        command = hadoopDir + "bin/shadoop index /" + file.getName() + " /index." + file.getName() + " -overwrite  sindex:str+ shape:hashtag blocksize:12.mb -no-local";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Copy to local
        command = hadoopDir + "bin/hadoop fs -copyToLocal" + " /index." + file.getName() + " " + indexDir + "hashtags/Day/";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }        //remove from hdfs 
        command = hadoopDir + "bin/hadoop fs -rmr" + " /" + file.getName();
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        command = hadoopDir + "bin/hadoop fs -rmr" + " /index." + file.getName();
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        
        AddSelectivityToMasterFile(indexDir + "hashtags/Day/"+" /index." + file.getName());

    }

    /**
     * This method create folder in hdfs
     *
     * @param folderName
     * @throws IOException
     * @throws InterruptedException
     */
    public void CreateHdfsFolder(String folderName) throws IOException, InterruptedException {
        command = hadoopDir + "bin/hadoop fs -mkdir /" + folderName;
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
    }

    /**
     * This copy file in hdfs under the given folder name
     *
     * @param folderName
     * @param fileDir
     * @throws IOException
     * @throws InterruptedException
     */
    public void CopytoHdfsFolder(String folderName, String fileDir) throws IOException, InterruptedException {
        command = hadoopDir + "bin/hadoop fs -copyFromLocal " + fileDir + " /" + folderName + "/";
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
    }

    /**
     * This method build the index of the hashtags under the given foldername
     * And copy the data to the local disk
     *
     * @param folderName
     * @throws IOException
     * @throws InterruptedException
     */
    public void BuildHashtagHdfsIndex(String folderName, String level) throws IOException, InterruptedException {
        File f = new File(indexDir + "hashtags/" + level + "/");
        if (!f.exists()) {
            f.mkdirs();
        }
        //Build index 
        command = hadoopDir + "bin/shadoop index /" + folderName + " /index." + folderName + " -overwrite  sindex:str+ shape:hashtag blocksize:12.mb -no-local";
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Copy to local
        command = hadoopDir + "bin/hadoop fs -copyToLocal" + " /index." + folderName + " " + indexDir + "hashtags/" + level + "/";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //remove from hdfs 
        command = hadoopDir + "bin/hadoop fs -rmr" + " /" + folderName;
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        command = hadoopDir + "bin/hadoop fs -rmr" + " /index." + folderName;
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        AddSelectivityToMasterFile(indexDir + "hashtags/" + level + "/index." + folderName);
    }

    /**
     * This method build the index of tweets under the given folderName And then
     * copy the index to local disk, Folder index must exist in the HDFS
     *
     * @param folderName
     * @throws IOException
     * @throws InterruptedException
     */
    public void BuildTweetHdfsIndex(String folderName, String level) throws IOException, InterruptedException {
        File f = new File(indexDir + "tweets/" + level + "/");
        if (!f.exists()) {
            f.mkdirs();
        }
        //Build index 
        command = hadoopDir + "bin/shadoop index /" + folderName + " /index." + folderName + " -overwrite  sindex:str+ shape:tweets blocksize:12.mb -no-local";
        System.out.println(command);
        Process myProcess = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //Copy to local
        command = hadoopDir + "bin/hadoop fs -copyToLocal" + " /index." + folderName + " " + indexDir + "tweets/" + level + "/";
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        //remove from hdfs 
        command = hadoopDir + "bin/hadoop fs -rmr" + " /" + folderName;
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        command = hadoopDir + "bin/hadoop fs -rmr" + " /index." + folderName;
        System.out.println(command);
        myProcess = Runtime.getRuntime().exec(command);
        in = new BufferedReader(
                new InputStreamReader(myProcess.getInputStream()));
        line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        AddSelectivityToMasterFile(indexDir + "tweets/" + level + "/index." + folderName);
    }

    /**
     * Level [Day,Week,Month] this method will update the tweets and hashtag
     * Lookup tables.
     *
     * @param level
     * @throws IOException
     */
    public void UpdatelookupTable(Level level) throws IOException {
        UpdatelookupTable("tweets", level, indexDir);
        UpdatelookupTable("hashtags", level, indexDir);
        UpdatelookupTable("tweets", level, invertedIndex);
        UpdatelookupTable("hashtags", level, invertedIndex);

    }

    private void UpdatelookupTable(String type, Level level, String Indexdirectory) throws IOException {
        System.out.println("Update lookupTable Type:" + type + " level:" + level.toString());
        File lookupTweet = new File(Indexdirectory + "/" + type + "/" + level.toString() + "/lookupTable.txt");
        System.out.println(lookupTweet);
        if (lookupTweet.exists()) {
            lookupTweet.delete();
        }
        OutputStreamWriter writer = new FileWriter(lookupTweet, true);
        File lookupDir = new File(Indexdirectory + "/" + type + "/" + level.toString() + "/");
        File[] indeces = lookupDir.listFiles();
        for (File f : indeces) {
            if (!f.getName().equals("lookupTable.txt")) {
                String[] fileName;
                fileName = f.getName().split("\\.");
//                writer.write(fileName[1] + ","+Indexdirectory+ type +"/" + level.toString() + "/" + f.getName() + "\n");
                writer.write(fileName[1] + "\n");

            }
        }
        writer.close();

    }

    public void createInvertedTweetIndex() throws IOException {
        List<File> file = new ArrayList<File>();
        File tweetFolder = new File(invertedIndex + "/tweets/Day/");
        if (!tweetFolder.exists()) {
            tweetFolder.mkdirs();
        }
        File tweetFile = new File(tweetsFile);
        file.add(tweetFile);
        //Create the inverted index
        if (new File(invertedIndex + "/tweets/Day/index." + tweetFile.getName()).exists()) {
            return;
        }
        KWIndexBuilder indexbuilder = new KWIndexBuilder();
        String indexfolder = invertedIndex + "/tweets/Day/index." + tweetFile.getName();
        indexbuilder.buildIndex(file, indexfolder, KWIndexBuilder.dataType.tweets);
        MetaData md = new MetaData();
        //create the meta data for the index 
        md.buildMetaData(invertedIndex + "/tweets/Day/index." + tweetFile.getName(),
                invertedIndex, tweetFile.getName());
    }

    public void createInvertedHashtagIndex() {
        List<File> file = new ArrayList<File>();
        File tweetFolder = new File(invertedIndex + "/hashtags/Day/");
        if (!tweetFolder.exists()) {
            tweetFolder.mkdirs();
        }
        File tweetFile = new File(hashtagsFile);
        file.add(tweetFile);
        //Create the inverted index
        if (new File(invertedIndex + "/hashtags/Day/index." + tweetFile.getName()).exists()) {
            return;
        }
        KWIndexBuilder indexbuilder = new KWIndexBuilder();
        String indexfolder = invertedIndex + "/hashtags/Day/index." + tweetFile.getName();
        indexbuilder.buildIndex(file, indexfolder, KWIndexBuilder.dataType.hashtags);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        String hadoopDir = "/home/turtle/workspace/hadoop-1.2.1/";
//        String tweetsFile = "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/tweets/2013-10-12";
//        String hashtagsFile = "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/hashtags/2013-10-12";
//        String indexDir = "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/result/rtreeindex/";
//        String invertedindex = "/home/turtle/UQUGIS/taghreed/scripts/IndexArme/result/invertedindex/";;
//        BuildIndex index = new BuildIndex(hadoopDir, tweetsFile, hashtagsFile, indexDir, invertedindex);
//        index.UpdatelookupTable(BuildIndex.Level.Day);
        BuildIndex in = new BuildIndex();
        in.AddSelectivityToMasterFile("/Users/louai/microblogsDataset/output/result/rtreeindex/tweets/Day/index.2014-01-02");
    }
}
