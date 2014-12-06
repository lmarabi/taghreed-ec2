package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Tweet;

public class GlobalPartitionContinents {

	private static Map<String,Integer> days = new HashMap<String, Integer>();
	private static OutputStreamWriter writer; 

	public static void main(String[] args) throws IOException, CompressorException, ParseException {
		Commons c = new Commons();
		File master;
		String fileString = System.getProperty("user.dir") + "/continent.txt";
		master = new File(fileString);
		BufferedReader reader = new BufferedReader(new FileReader(master));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] token = line.split(",");
			String continentName = token[0];
			MBR mbr = new MBR(token[1]);
			//init writer
			File folder = new File(System.getProperty("user.dir")+"/"+continentName);
			if(!folder.exists()){
				folder.mkdirs();
			}
			writer = new OutputStreamWriter(
					new FileOutputStream(System.getProperty("user.dir")+"/"+continentName+ "/_inputDataStatistics_" + ".txt", false), "UTF-8");
			//read from the files and output it to the folder 
			List<File> files = listf(c.getTweetFlushDir());
			int lineCount = 0;
			for(int i=0; i< files.size(); i++){
				String outputfile = System.getProperty("user.dir")+"/"+continentName+"/"+files.get(i).getName().replace(".bz2", "");
				lineCount = devideSpace(mbr, files.get(i), outputfile);
				String temp = files.get(i).getName().toString().replace(".bz2","");
				days.put(temp, lineCount);
				System.out.println(temp+" - "+lineCount+" processed "+continentName+":"+i+"/"+files.size());
				writer.write(temp+"_"+lineCount+"\n");
			}
			writer.close();
			
		}
		reader.close();
		
		/* This code counts the number of tweets each day
		 writer = new OutputStreamWriter(
					new FileOutputStream(System.getProperty("user.dir") + "/_inputDataStatistics_" + ".txt", false), "UTF-8");
		List<File> files = listf(c.getTweetFlushDir());
		int lineCount = 0;
		for(int i=0; i< files.size(); i++){
			lineCount = countRecordsInDay(files.get(i));
			String temp = files.get(i).getName().toString().replace(".bz2","");
			days.put(temp, lineCount);
			System.out.println(temp+" - "+lineCount+" processed:"+i+"/"+files.size());
			writer.write(temp+"_"+lineCount+"\n");
		}
		writer.close();
		*/

	}
	
	private static int devideSpace(MBR mbr, File file,String outputfile) throws CompressorException, IOException, ParseException{
		int count = 0;
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(outputfile, false), "UTF-8");
		BufferedReader reader = getBufferedReaderForBZ2File(file.getPath());
		String line = null;
		while((line = reader.readLine())!= null){
			Tweet tweetobj = new Tweet(line);
			Point position = new Point(tweetobj.lat,tweetobj.lon);
			if(mbr.insideMBR(position)){
				count++;
				//writer to other file.
				writer.write(line);
			}
			
			
		}
		writer.close();
		reader.close();
		return count;
	}
	
	private static int countRecordsInDay(File file) throws CompressorException, IOException{
		int count = 0;
		BufferedReader reader = getBufferedReaderForBZ2File(file.getPath());
		String line = null;
		while((line = reader.readLine())!= null){
			count++;
		}
		reader.close();
		return count;
	}
	
	public static BufferedReader getBufferedReaderForBZ2File(String fileIn) throws FileNotFoundException, CompressorException {
	    FileInputStream fin = new FileInputStream(fileIn);
	    BufferedInputStream bis = new BufferedInputStream(fin);
	    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
	    BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
	    return br2;
	}
	
	public static List<File> listf(String directoryName) {
		File directory = new File(directoryName);

		List<File> resultList = new ArrayList<File>();

		// get all the files from a directory
		File[] fList = directory.listFiles();
		resultList.addAll(Arrays.asList(fList));
		for (File file : fList) {
			if (file.isFile()) {

			} else if (file.isDirectory()) {
				resultList.addAll(listf(file.getAbsolutePath()));
			}
		}
		// System.out.println(fList);
		return resultList;
	}

}