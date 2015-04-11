package umn.ec2.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.gistic.taghreed.collections.PyramidMonth;
import org.gistic.taghreed.collections.PyramidWeek;

public class PrepareData {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		arrangeWeeks();
		arrangeMonth();

	}

	private static void arrangeWeeks() throws Exception {
		//System.out.println("Create Tweets Week Index ");
		List<File> outputFiles = ListFiles("/media/louai/My Book/dayTweets/");
		Collections.sort(outputFiles);
		Calendar c = Calendar.getInstance();
		PyramidWeek temp = new PyramidWeek();
		String command = "";
		HashMap<String, List<File>> indeces = temp.buildWeekIndex(outputFiles);
		Iterator it = indeces.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, List<File>> week = (Entry<String, List<File>>) it
					.next();

			//System.out.println("*******\nFound week\n**********");
			String hadoopOutputFolder = week.getKey();
			//System.out.println(hadoopOutputFolder);
			File indexFolder = new File("/media/louai/My Book/Week/"
					+ hadoopOutputFolder + "/");
			if (!indexFolder.exists()) {
				// Create folder in hdfs with hadoopoutputFolder
				indexFolder.mkdirs();
				// Copy data to hdfs under hadoopoutputfolder
				for (File day : week.getValue()) {
					//System.out.println("copy... : " + day);
				    command = "cp "+day.getAbsolutePath().replace(" ", "\\ ")+" /media/louai/My\\ Book/Week/"
							+ hadoopOutputFolder + "/";
				    commandExecuter(command);
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
	private static List<File> ListFiles(String directoryName) {
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

	public static void commandExecuter(String command)
			throws IOException, InterruptedException {
			System.out.println(command);
//		Process myProcess = Runtime.getRuntime().exec(command);
//		StreamGobbler errorGobbler = new StreamGobbler(
//				myProcess.getErrorStream(),//System.out);
//
//		// Any output?
//		StreamGobbler outputGobbler = new StreamGobbler(
//				myProcess.getInputStream(),System.err);
//
//		errorGobbler.start();
//		outputGobbler.start();
//
//		// Any error
//		int exitVal = myProcess.waitFor();
//		errorGobbler.join(); // Handle condition where the
//		outputGobbler.join(); // process ends before the threads finish
	}

	private static void arrangeMonth() throws IOException, InterruptedException, ParseException {
		Map<Integer, PyramidMonth> indexMonth = new HashMap<Integer, PyramidMonth>();
//		//System.out.println("Create Tweets Months Index ");
        List<File> outputFiles = ListFiles("/media/louai/My Book/dayTweets/");
        Collections.sort(outputFiles);
        Calendar c = Calendar.getInstance();
        PyramidMonth tempMonth = new PyramidMonth();
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

//        //System.out.println("------------");
        String command = "";
        Iterator it = indexMonth.entrySet().iterator();
        String currentMonth = c.getTime().getYear() +"-"+(c.getTime().getMonth()+1);
        while (it.hasNext()) {
            Map.Entry obj = (Map.Entry) it.next();
//            //System.out.println("*** " + obj.getKey() + " ***");
            PyramidMonth month = (PyramidMonth) obj.getValue();
            String monthName = "";
            if((month.getMonth() + 1) <10){
            	monthName = "0"+ (month.getMonth() + 1);
            }else{
            	monthName = String.valueOf((month.getMonth() + 1));
            }
            String hadoopOutputFolder = month.getYear() + "-" + monthName;
            //Create folder in hdfs with hadoopoutputFolder
            File indexFolder = new File("/media/louai/My Book/Month/" + hadoopOutputFolder+"/");
            if (!indexFolder.exists() && !currentMonth.equals(hadoopOutputFolder)) {
                indexFolder.mkdirs();
                for (File day : month.getFiles()) {
                    command = "cp "+day.getAbsolutePath().replace(" ", "\\ ")+" /media/louai/My\\ Book/Month/" + hadoopOutputFolder+"/";
                    commandExecuter(command);
                }
            } else {
                //System.out.println("Index exist " + hadoopOutputFolder);
            }

        }
	}

}

class StreamGobbler extends Thread {
	InputStream is;
	PrintStream os;

	StreamGobbler(InputStream is, PrintStream os) throws UnsupportedEncodingException, FileNotFoundException {
		this.is = is;
		this.os = os;
		
	}

	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is));
			 String line = null;
			 while ((line = reader.readLine()) != null) {
				 os.println(line);
			 }
		}catch (IOException ioe) {
			//handel error.
		}
		
	}
}
