/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseIndexer;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorException;
import org.gistic.invertedIndex.KWIndexBuilder;
import org.gistic.invertedIndex.MetaData;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.collections.PyramidMonth;
import org.gistic.taghreed.collections.PyramidWeek;

/**
 *
 * @author turtle
 */
public class BuildPyramidIndex {

    //Server directory
	private Commons config;
    
    //Local test
//    private String hadoopDir = "/home/turtle/workspace/hadoop-1.2.1/";
//    private String tweetsDir = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/tweets/";
//    private String hashtagsDir = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/hashtags/";
//    private String rtreeindexDir = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/invertedindex/";
    BuildIndex indexer ;
    private Map<Integer, PyramidMonth> indexMonth = new HashMap<Integer, PyramidMonth>();

    public BuildPyramidIndex() throws IOException {
        this.indexer = new BuildIndex();
        this.config = new Commons();
        
    }
    

    /**
     * Create the week pyramid indexMonth for both tweets and hashtags
     * @throws CompressorException 
     */
    public void CreateIndex() throws IOException, InterruptedException, ParseException, CompressorException {
        //Create the rtree index using hadoop
        CreateRtreeTweetWeekIndex();
//        CreateRtreeHashtagWeekIndex();
        createRtreeTweetMonths();
//        createRtreeHashtagMonths();
        //Create the inverted index 
//        CreateInvertedTweetWeekIndex();
//        createInvertedTweetMonths();
//        CreateInvertedHashtagWeekIndex();
//        createInvertedHashtagMonths();
        //update the lookup tables for all indeces
        indexer.UpdatelookupTable(BuildIndex.Level.Day);
        indexer.UpdatelookupTable(BuildIndex.Level.Week);
        indexer.UpdatelookupTable(BuildIndex.Level.Month);
        
        // need to update the inverted index lookup table
    }
    
    /**
     * This method create weeks inverted index 
     * @throws ParseException
     * @throws IOException
     * @throws InterruptedException 
     * @throws CompressorException 
     */
    private void CreateInvertedTweetWeekIndex() throws ParseException, IOException, InterruptedException, CompressorException{
        System.out.println("Create Tweets Week Index ");
        List<File> outputFiles = ListFiles(config.getTweetFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        PyramidWeek temp = new PyramidWeek();
        boolean firstF = true;
        for (File f : outputFiles) {
            if (firstF) {
                temp = new PyramidWeek(f.getName());
                temp.addNewFile(f);
                firstF = false;
            } else {
                if (temp.isTheSameWeek(f.getName())) {
                    temp.addNewFile(f);
                } else {
                    //if not in the same week of the year then index the founded
                    //week and then create a new list.
                    if (temp.getFiles().size() >= 5) {
                        System.out.println("*******\nFound week\n**********");
                        String hadoopOutputFolder = temp.getFirstDayOfWeek().replace(".bz2", "")
                                + "&" + temp.getLastDayofWeek().replace(".bz2", "");
                        System.out.println(hadoopOutputFolder);
                        String weekdir = config.getQueryInvertedIndex() + "tweets/Week/index." + hadoopOutputFolder;
                        File indexFolder = new File(weekdir);
                        if (!indexFolder.exists()) {
                            //Create the inverted index 
                            KWIndexBuilder kWIndexBuilder = new KWIndexBuilder();
                            boolean status = kWIndexBuilder.buildIndex(temp.getFiles(), weekdir,KWIndexBuilder.dataType.tweets);
                            System.out.println("status: "+status+" "+weekdir);
                            MetaData md = new MetaData();
                    		// create the meta data for the index
                    		md.buildMetaData(weekdir,
                    				config.getQueryInvertedIndex(), hadoopOutputFolder,
                    				BuildIndex.getThreshold(config.getQueryRtreeIndex()+ "tweets/Week/index."+ hadoopOutputFolder.replace(".bz2", "")));
                        }
                    }

                    temp = new PyramidWeek(f.getName());
                    temp.addNewFile(f);
                }
            }

        }
    }
    
    private void CreateInvertedHashtagWeekIndex() throws ParseException, IOException, InterruptedException, CompressorException{
        System.out.println("Create Tweets Week Index ");
        List<File> outputFiles = ListFiles(config.getHashtagFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        PyramidWeek temp = new PyramidWeek();
        boolean firstF = true;
        for (File f : outputFiles) {
            if (firstF) {
                temp = new PyramidWeek(f.getName());
                temp.addNewFile(f);
                firstF = false;
            } else {
                if (temp.isTheSameWeek(f.getName())) {
                    temp.addNewFile(f);
                } else {
                    //if not in the same week of the year then index the founded
                    //week and then create a new list.
                    if (temp.getFiles().size() >= 5) {
                        System.out.println("*******\nFound week\n**********");
                        String hadoopOutputFolder = temp.getFirstDayOfWeek()
                                + "&" + temp.getLastDayofWeek();
                        System.out.println(hadoopOutputFolder);
                        String weekdir = config.getQueryInvertedIndex() + "hashtags/Week/index." + hadoopOutputFolder;
                        File indexFolder = new File(weekdir);
                        if (!indexFolder.exists()) {
                            //Create the inverted index 
                            KWIndexBuilder kWIndexBuilder = new KWIndexBuilder();
                            boolean status = kWIndexBuilder.buildIndex(temp.getFiles(), weekdir,KWIndexBuilder.dataType.hashtags);
                            System.out.println("status: "+status+" "+weekdir);
                        }
                    }

                    temp = new PyramidWeek(f.getName());
                    temp.addNewFile(f);
                }
            }

        }
    }
    
    
    /**
     * This method create Months inverted index
     * @throws IOException
     * @throws InterruptedException
     * @throws ParseException 
     * @throws CompressorException 
     */
    private void createInvertedTweetMonths() throws IOException, InterruptedException, ParseException, CompressorException {
        System.out.println("Create Tweets Months Index ");
        List<File> outputFiles = ListFiles(config.getTweetFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        
        PyramidMonth tempMonth = new PyramidMonth();
        boolean flag = false;
        for (File f : outputFiles) {
            tempMonth = new PyramidMonth(f.getName().replace(".bz2", ""));
            if (indexMonth.containsKey(tempMonth.getKey())) {
            		tempMonth = indexMonth.get(tempMonth.getKey());
            		tempMonth.addFile(f);
            		indexMonth.put(tempMonth.getKey(), tempMonth);
            } else {
                tempMonth.addFile(f);
                indexMonth.put(tempMonth.getKey(), tempMonth);
            }

        }

        System.out.println("------------");
        Iterator it = indexMonth.entrySet().iterator();
        String currentMonth = c.getTime().getYear() +"-"+(c.getTime().getMonth()+1);
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            System.out.println("*** " + obj.getKey() + " ***");
            PyramidMonth month = (PyramidMonth) obj.getValue();
            String folderName = month.getYear() + "-" + (month.getMonth() + 1);
            System.out.println(folderName);
            String indexedMonth = month.getYear() + "-" + (month.getMonth()+1);
            //Create folder in hdfs with hadoopoutputFolder
            String Monthdir = config.getQueryInvertedIndex() + "tweets/Month/index." + folderName;
            File indexFolder = new File(Monthdir);
            if (!indexFolder.exists() && !currentMonth.equals(indexedMonth)) {
                //build the index
                KWIndexBuilder builder = new KWIndexBuilder();
                boolean status = builder.buildIndex(month.getFiles(), Monthdir,KWIndexBuilder.dataType.tweets);
                System.out.println("status: "+status+" "+Monthdir);
                MetaData md = new MetaData();
        		// create the meta data for the index
        		md.buildMetaData(Monthdir,
        				config.getQueryInvertedIndex(), folderName,
        				BuildIndex.getThreshold(config.getQueryRtreeIndex()+ "tweets/Month/index."+ folderName.replaceAll(".bz2", "")));
            } else {
                System.out.println("Index exist " + folderName);
            }

        }
    }
/*
    
     * This method create Months inverted index
     * @throws IOException
     * @throws InterruptedException
     * @throws ParseException 
     * @throws CompressorException 
     
    private void createInvertedHashtagMonths() throws IOException, InterruptedException, ParseException, CompressorException {
        System.out.println("Create Tweets Months Index ");
        List<File> outputFiles = ListFiles(config.getHashtagFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        
        PyramidWeek tempObj = new PyramidWeek();
        boolean flag = false;
        for (File f : outputFiles) {
            tempObj = new PyramidWeek(f.getName());
            if (indexMonth.containsKey(tempObj.getMonth())) {
                Integer key = tempObj.getMonth();
                tempObj = indexMonth.get(key);
                tempObj.addNewFile(f);
                indexMonth.put(key, tempObj);
            } else {
                tempObj.addNewFile(f);
                indexMonth.put(tempObj.getMonth(), tempObj);
            }

        }

        System.out.println("------------");
        Iterator it = indexMonth.entrySet().iterator();
        String currentMonth = c.getTime().getYear() +"-"+(c.getTime().getMonth()+1);
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            System.out.println("*** " + obj.getKey() + " ***");
            PyramidWeek week = (PyramidWeek) obj.getValue();
            String folderName = week.getYear() + "-" + (week.getMonth() + 1);
            System.out.println(folderName);
            String indexedMonth = week.getYear() + "-" + (week.getMonth()+1);
            //Create folder in hdfs with hadoopoutputFolder
            String Monthdir = config.getQueryInvertedIndex() + "hashtags/Month/index." + folderName;
            File indexFolder = new File(Monthdir);
            if (!indexFolder.exists() && !currentMonth.equals(indexedMonth)) {
                //build the index
                KWIndexBuilder builder = new KWIndexBuilder();
                boolean status = builder.buildIndex(week.getFiles(), Monthdir,KWIndexBuilder.dataType.hashtags);
                System.out.println("status: "+status+" "+Monthdir);
            } else {
                System.out.println("Index exist " + folderName);
            }

        }
    }
*/
    
    /**
     * This method create tweets indexMonth only
     */
    private void CreateRtreeTweetWeekIndex() throws IOException, InterruptedException, ParseException {
        System.out.println("Create Tweets Week Index ");
        List<File> outputFiles = ListFiles(config.getTweetFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        PyramidWeek temp = new PyramidWeek();
        boolean firstF = true;
        for (File f : outputFiles) {
            if (firstF) {
                temp = new PyramidWeek(f.getName());
                temp.addNewFile(f);
                firstF = false;
            } else {
                if (temp.isTheSameWeek(f.getName().replace(".bz2", ""))) {
                    temp.addNewFile(f);
                } else {
                    //if not in the same week of the year then index the founded
                    //week and then create a new list.
                    if (temp.getFiles().size() >= 5) {
                        System.out.println("*******\nFound week\n**********");
                        String hadoopOutputFolder = temp.getFirstDayOfWeek().replace(".bz2", "")
                                + "&" + temp.getLastDayofWeek().replace(".bz2", "");
                        System.out.println(hadoopOutputFolder);
                        File indexFolder = new File(config.getQueryRtreeIndex() + "tweets/Week/index." + hadoopOutputFolder);
                        if (!indexFolder.exists()) {
                            //Create folder in hdfs with hadoopoutputFolder
                            indexer.CreateHdfsFolder(hadoopOutputFolder);
                            //Copy data to hdfs under hadoopoutputfolder
                            for (File day : temp.getFiles()) {
                                indexer.CopytoHdfsFolder(hadoopOutputFolder, day.getAbsolutePath());
                            }
                            //build the index for tweets and copy to local
                            indexer.BuildTweetHdfsIndex(hadoopOutputFolder, "Week");
                        }
                    }

                    temp = new PyramidWeek(f.getName());
                    temp.addNewFile(f);
                }
            }

        }
    }

    /**
     * This method create tweets indexMonth only
     */
    private void CreateRtreeHashtagWeekIndex() throws IOException, InterruptedException, ParseException {
        System.out.println("Create Hashtags Week Index ");
        List<File> outputFiles = ListFiles(config.getHashtagFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        PyramidWeek temp = new PyramidWeek();
        boolean firstF = true;
        for (File f : outputFiles) {
            if (firstF) {
                temp = new PyramidWeek(f.getName());
                temp.addNewFile(f);
                firstF = false;
            } else {
                if (temp.isTheSameWeek(f.getName().replace(".bz2", ""))) {
                    temp.addNewFile(f);
                } else {
                    //if not in the same week of the year then index the founded
                    //week and then create a new list.
                    if (temp.getFiles().size() >= 5) {
                        System.out.println("*******\nFound week\n**********");
                        String hadoopOutputFolder = temp.getFirstDayOfWeek().replace(".bz2", "")
                                + "&" + temp.getLastDayofWeek().replace(".bz2", "");
                        File indexFolder = new File(config.getQueryRtreeIndex() + "hashtags/Week/index." + hadoopOutputFolder);
                        if (!indexFolder.exists()) {
                            //Create folder in hdfs with hadoopoutputFolder
                            indexer.CreateHdfsFolder(hadoopOutputFolder);
                            //Copy data to hdfs under hadoopoutputfolder
                            for (File day : temp.getFiles()) {
                                indexer.CopytoHdfsFolder(hadoopOutputFolder, day.getAbsolutePath());
                            }
                            //build the index for tweets and copy to local
                            indexer.BuildHashtagHdfsIndex(hadoopOutputFolder, "Week");
                        }
                    }

                    temp = new PyramidWeek(f.getName());
                    temp.addNewFile(f);
                }
            }

        }
    }

    /**
     * This Method takes a directory and return all files in the directory as
     * List<File>
     *
     * @param directoryName
     * @return List<File>
     */
    private List<File> ListFiles(String directoryName) {
        File directory = new File(directoryName);

        List<File> resultList = new ArrayList<File>();

        // get all the files from a directory
        File[] fList = directory.listFiles();
        resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
            } else if (file.isDirectory()) {
                resultList.addAll(ListFiles(file.getAbsolutePath()));
            }
        }
        Collections.sort(resultList);
        return resultList;
    }

    private void createRtreeTweetMonths() throws IOException, InterruptedException, ParseException {
        System.out.println("Create Tweets Months Index ");
        List<File> outputFiles = ListFiles(config.getTweetFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        int weekOfMonth = 0;
        PyramidMonth tempMonth = new PyramidMonth();
        boolean flag = false;
        for (File f : outputFiles) {
            tempMonth = new PyramidMonth(f.getName().replace(".bz2", ""));
            if (indexMonth.containsKey(tempMonth.getKey())) {
            		tempMonth = indexMonth.get(tempMonth.getKey());
            		tempMonth.addFile(f);
            		indexMonth.put(tempMonth.getKey(), tempMonth);
            } else {
                tempMonth.addFile(f);
                indexMonth.put(tempMonth.getKey(), tempMonth);
            }

        }

        System.out.println("------------");
        Iterator it = indexMonth.entrySet().iterator();
        String currentMonth = c.getTime().getYear() +"-"+(c.getTime().getMonth()+1);
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            System.out.println("*** " + obj.getKey() + " ***");
            PyramidMonth month = (PyramidMonth) obj.getValue();
            String hadoopOutputFolder = month.getYear() + "-" + (month.getMonth() + 1);
            System.out.println(hadoopOutputFolder);
            String indexedMonth = month.getYear() + "-" + (month.getMonth()+1);
            //Create folder in hdfs with hadoopoutputFolder
            File indexFolder = new File(config.getQueryRtreeIndex() + "tweets/Month/index." + hadoopOutputFolder);
            if (!indexFolder.exists() && !currentMonth.equals(indexedMonth)) {
                indexer.CreateHdfsFolder(hadoopOutputFolder);
                for (File day : month.getFiles()) {
                    indexer.CopytoHdfsFolder(hadoopOutputFolder, day.getAbsolutePath());
                }
                //build the index for tweets and copy to local
                indexer.BuildTweetHdfsIndex(hadoopOutputFolder, "Month");
            } else {
                System.out.println("Index exist " + hadoopOutputFolder);
            }

        }
    }

    /*
    private void createRtreeHashtagMonths() throws IOException, InterruptedException, ParseException {
        System.out.println("Create Hashtags Months Index ");
        List<File> outputFiles = ListFiles(config.getHashtagFlushDir());
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        int weekOfMonth = 0;
        PyramidWeek tempObj = new PyramidWeek();
        boolean flag = false;
        for (File f : outputFiles) {
            tempObj = new PyramidWeek(f.getName());
            if (indexMonth.containsKey(tempObj.getMonth())) {
                Integer key = tempObj.getMonth();
                tempObj = indexMonth.get(key);
                tempObj.addNewFile(f);
                indexMonth.put(key, tempObj);
            } else {
                tempObj.addNewFile(f);
                indexMonth.put(tempObj.getMonth(), tempObj);
            }

        }

        System.out.println("------------");
        Iterator it = indexMonth.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
            System.out.println("*** " + obj.getKey() + " ***");
            PyramidWeek week = (PyramidWeek) obj.getValue();
            String hadoopOutputFolder = week.getYear() + "-" + (week.getMonth() + 1);
            System.out.println(hadoopOutputFolder);
            File indexFolder = new File(config.getQueryRtreeIndex() + "hashtags/Month/index." + hadoopOutputFolder);
            if (!indexFolder.exists()) {
                //Create folder in hdfs with hadoopoutputFolder
                indexer.CreateHdfsFolder(hadoopOutputFolder);
                for (File day : week.getFiles()) {
                    indexer.CopytoHdfsFolder(hadoopOutputFolder, day.getAbsolutePath());
                }
                //build the index for tweets and copy to local
                indexer.BuildHashtagHdfsIndex(hadoopOutputFolder, "Month");
            }
        }
    }
*/
    public static void main(String[] args) throws IOException, InterruptedException, ParseException, CompressorException {
        BuildPyramidIndex x = new BuildPyramidIndex();
        x.createRtreeTweetMonths();
//        x.indexer.UpdatelookupTable("Day");
//        x.indexer.UpdatelookupTable("Week");
//        x.indexer.UpdatelookupTable("Month");
        System.out.println("End program");
    }
}
