/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.gistic.invertedIndex.KWIndexSearcher;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.Hashtag;
import org.gistic.taghreed.collections.Partition;
import org.gistic.taghreed.collections.TopTweetResult;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.Week;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQueryOptimizer.GridCell;
import org.gistic.taghreed.spatialHadoop.Tweets;

import com.jcraft.jsch.jce.Random;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;

/**
 *
 * @author turtle
 */
public class QueryExecutor {

	// Global lookup
	private Lookup lookup = new Lookup();
	private static double startTime;
	private static double endTime;
	private Date startDate;
	private Date endDate;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static ServerRequest serverRequest;

	public final static TopTweetResult result = new TopTweetResult(100);

	public QueryExecutor(ServerRequest request) throws IOException,
			FileNotFoundException, ParseException {
		this.serverRequest = request;
		this.lookup = request.getLookup();
	}

	public QueryExecutor(String startDate, String endDate)
			throws ParseException {
		this.startDate = dateFormat.parse(startDate);
		this.endDate = dateFormat.parse(endDate);
	}

	/**
	 * This metho will read from the nodes inside the R-tree index
	 *
	 * @param id
	 * @param maxLat
	 * @param maxLon
	 * @param minLat
	 * @param minLon
	 * @param dataPath
	 * @param exportPath
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws CompressorException
	 */
	public void GetSmartOutput(String day, String dataPath)
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException, InterruptedException {
		int count = 0;
		// Set the area of interest in the MBR
		dataPath += "/";
		Thread[] readThreads = null;
		if (serverRequest.getIndex().equals(ServerRequest.queryIndex.rtree)) {
			// Get the set of Files that intersect with the area.

			List<Partition> files = ReadMaster(day, dataPath);
			logEnd("selected (" + files.size() + ")");
			readThreads = new Thread[files.size()];
			int index = 0;
			// read eachfile and output the result.
			for (Partition f : files) {
				System.out.println("Start Reading file "
						+ f.getPartition().getName()+"\n"
						+f.getArea().toWKT());
				readThreads[index] = new Thread(new smartQueryThread(f));
				readThreads[index].start();
				index++;

			}
		} else {
			count += GetDocumentsInvertedIndex(dataPath);
		}
		logEnd("end reading files");
		if (readThreads != null) {
			for (int i = 0; i < readThreads.length; i++) {
				while (readThreads[i].isAlive()) {
					// wait until thread finished reading files
				}
			}
		}
	}

	/**
	 * This metho will read from the nodes inside the R-tree index
	 *
	 * @param id
	 * @param maxLat
	 * @param maxLon
	 * @param minLat
	 * @param minLon
	 * @param dataPath
	 * @param exportPath
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws CompressorException
	 */
	public void GetSmartOutputCheckTemporal(String day, String dataPath)
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		dataPath += "/";
		// Get the set of Files that intersect with the area.
		logStart("start reading the master files");
		List<Partition> files = ReadMaster(day, dataPath);
		logEnd("end reading master file and selected (" + files.size() + ")");
		// read eachfile and output the result.
		logStart("start reading from selected files");
		for (Partition f : files) {
			System.out.println("Start Reading file "
					+ f.getPartition().getName());
			smartQueryChechTemporal(f);
			System.out
					.println("End reading file " + f.getPartition().getName());
		}

		logEnd("end reading files");
	}

	/**
	 * This method Write the Final result .
	 *
	 * @param f
	 * @param out
	 *            Streamer
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static synchronized int smartQuery(Partition part)
			throws FileNotFoundException, IOException, ParseException {
		BufferedReader reader;
		// FileInputStream fin = new FileInputStream(part.getPartition());
		// BufferedInputStream bis = new BufferedInputStream(fin);
		// CompressorInputStream input = new
		// CompressorStreamFactory().createCompressorInputStream(bis);
		// BufferedReader reader = new BufferedReader(new
		// InputStreamReader(input,"UTF-8"));
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				part.getPartition()), "UTF-8"));
		String tweet;
		int count = 0;
		// get the range file
		while ((tweet = reader.readLine()) != null) {
			// Here rather than wrting to local storage you can pass it
			// right away to Visualization team.

			Point node;
			if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
				Tweet tweetObj = new Tweet(tweet);
				node = new Point(tweetObj.lat, tweetObj.lon);
				if (serverRequest.getMbr().insideMBR(node)) {
					if (serverRequest.getQuery() != null) {
						if (tweetObj.tweetText.contains(serverRequest
								.getQuery())) {
//							result.put(tweetObj);
							// output.write(tweet);
							// output.write("\n");
							count++;
							// insertTweetsToVolume(tweetObj.created_at);
						}
					} else {
//						result.put(tweetObj);
						// output.write(tweet);
						// output.write("\n");
						count++;
						// insertTweetsToVolume(tweetObj.created_at);
					}

				}
			} else {
				Hashtag hashtag = new Hashtag(tweet);
				node = new Point(hashtag.getLat(), hashtag.getLon());
				if (serverRequest.getMbr().insideMBR(node)) {
					if (serverRequest.getQuery() != null) {
						if (hashtag.getHashtagText().contains(
								serverRequest.getQuery())) {
							// output.write(tweet);
							// output.write("\n");
							count++;
						}
					} else {
						// output.write(tweet);
						// output.write("\n");
						count++;
					}

				}
			}

		}
		reader.close();
		return count;
	}

	public class smartQueryThread implements Runnable {
		Partition p;

		public smartQueryThread(Partition p) {
			this.p = p;
		}

		@Override
		public void run() {
			try {
				smartQuery(p);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * This method Write the Final result .
	 *
	 * @param f
	 * @param out
	 *            Streamer
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void smartQueryChechTemporal(Partition part)
			throws FileNotFoundException, IOException, ParseException {

		BufferedReader reader;
		// FileInputStream fin = new FileInputStream(part.getPartition());
		// BufferedInputStream bis = new BufferedInputStream(fin);
		// CompressorInputStream input = new
		// CompressorStreamFactory().createCompressorInputStream(bis);
		// BufferedReader reader = new BufferedReader(new
		// InputStreamReader(input,"UTF-8"));
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(
				part.getPartition()), "UTF-8"));
		String tweet;
		// get the range file
		while ((tweet = reader.readLine()) != null) {
			// Here rather than wrting to local storage you can pass it
			// right away to Visualization team.
			String[] temp = tweet.split(",");
			Point node;
			if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
				node = new Point(temp[8], temp[9]);
			} else {
				node = new Point(temp[0], temp[1]);
			}
			if (serverRequest.getMbr().insideMBR(node)) {
				if (serverRequest.getType().equals(
						ServerRequest.queryType.tweet)) {
					String[] datetemp = temp[0].split(" ");
					Date date = dateFormat.parse(datetemp[0]);
					if (org.gistic.taghreed.diskBaseQuery.query.Lookup
							.insideDaysBoundry(
									dateFormat.format(this.startDate),
									dateFormat.format(this.endDate),
									dateFormat.format(date))) {
						// output.write(tweet);
						// output.write("\n");
					}
				} else {
					// output.write(tweet);
					// output.write("\n");
				}

			}

		}
		reader.close();
	}

	/**
	 * output message and start a timer to check the time
	 *
	 * @param text
	 */
	public static void logStart(String text) {
		startTime = System.currentTimeMillis();
		System.out.println(text);
	}

	/**
	 * End the message and show the time in second
	 *
	 * @param text
	 */
	public static void logEnd(String text) {
		endTime = System.currentTimeMillis();
		System.out.println(text + " which took" + " "
				+ ((endTime - startTime) / 1000) + " seconds");
	}

	/**
	 * This Method read all the files in the data directory and fetch only the
	 * Intersect files
	 *
	 * @param maxLat
	 * @param minLat
	 * @param maxLon
	 * @param minLon
	 * @param path
	 * @return
	 */
	private static List<Partition> ReadMaster(String day, String path)
			throws FileNotFoundException, IOException {
		File master;
		List<Partition> result = new ArrayList<Partition>();
		// check the master files with the index used at the backend
		master = new File(path + "/_master.quadtree");
		if (!master.exists()) {
			master = new File(path + "/_master.str");
			if (!master.exists()) {
				master = new File(path + "/_master.str+");
				if (!master.exists()) {
					master = new File(path + "/_master.grid");
				}
			}
		}

		BufferedReader reader = new BufferedReader(new FileReader(master));
		// FileInputStream fin = new FileInputStream(master);
		// BufferedInputStream bis = new BufferedInputStream(fin);
		// CompressorInputStream input = new
		// CompressorStreamFactory().createCompressorInputStream(bis);
		// BufferedReader reader = new BufferedReader(new
		// InputStreamReader(input, "UTF-8"));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split(",");
			// The file has the following format as Aggreed with the interface
			// between hadoop and this program
			// #filenumber,minLat,minLon,maxLat,maxLon
			// 0,minLon,MinLat,MaxLon,MaxLat,Filename
			if (temp.length == 8) {
				Partition part = new Partition(line, path, day);
//				System.out.println(part.getPartition().getName()+"\t"+part.getArea().toWKT());
				if (serverRequest.getMbr().Intersect(part.getArea().getMax(),
						part.getArea().getMain())) {
					result.add(part);
				}
			}
		}
		reader.close();
		return result;
	}

	/**
	 * @param args
	 *            the command line arguments
	 * @throws InterruptedException
	 */
	public TopTweetResult executeQuery() throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException, InterruptedException {
		Map<String, String> index = null;
		double startTime = System.currentTimeMillis();
		Rectangle mbr = new Rectangle(serverRequest.getMbr().getMin().getLat(),serverRequest.getMbr().getMin().getLon(), serverRequest.getMbr().getMax().getLat(),serverRequest.getMbr().getMax().getLon());
		if(this.serverRequest.getQueryResolution().equals(queryLevel.Day)){
			index = getIndexArmy();
		}else if(this.serverRequest.getQueryResolution().equals(queryLevel.Month)){
			index = getMonthTree();
		}else if(this.serverRequest.getQueryResolution().equals(queryLevel.Week)){
			 index = getWeekTree();
		}
		
		
		System.out.println("#number of dates found: " + index.size());
		Iterator it = index.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			System.out.println("#Start Reading index of "
					+ entry.getKey().toString());

//			GetSmartOutput(entry.getKey().toString(), entry.getValue()
//					.toString());
			
			long count = executeRangeQuery(mbr, entry.getValue().toString());
			System.out.println("Result reterived : "+count);



		}

		double endTime = System.currentTimeMillis();
		System.out.println("query time = " + (endTime - startTime) + " ms");


		
		return this.result;

	}

	private static long executeRangeQuery(Rectangle mbr, String path)
			throws IllegalArgumentException, IOException {
		long count;
		final List<Tweets> list = new ArrayList<Tweets>();
		Random r = new Random(); 
		count = RangeQuery.rangeQueryLocal(new Path(path), mbr, new Tweets(),
				new OperationsParams(), new ResultCollector<Tweets>() {

					@Override
					public void collect(Tweets arg0) {
						if(arg0 != null)
							result.put(arg0);
							

					}
				});
		return count;
	}

	public GridCell readMastersFile() throws UnsupportedEncodingException,
			FileNotFoundException, IOException, ParseException {

		Map<String, String> index = getIndexArmy();
		// System.out.println("#number of dates found: " + index.size());
		Iterator it = index.entrySet().iterator();
		long count = 0;
		GridCell cell = new GridCell(this.serverRequest.getMbr(), lookup);
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			// System.out.println("#Start Reading index of "
			// + entry.getKey().toString());
			// if(lookup.isDayFromMissingDay((entry.getKey().toString()))){
			// continue;
			// }
			List<Partition> file = ReadMaster(entry.getKey().toString(), entry
					.getValue().toString() + "/");
			for (Partition p : file) {
				cell.add(p.getDay(), p.getCardinality());
			}

		}
		return cell;
	}

	/**
	 * This method read the Grid cell from the Masters Files.
	 * 
	 * @param day
	 * @param dataPath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public long readMasterFile(String day, String dataPath)
			throws FileNotFoundException, IOException {
		dataPath += "/";
		long cardinality = 0;
		// Get the set of Files that intersect with the area.
		List<Partition> files = ReadMaster(day, dataPath);
		// Get the partitions information and put it in a cell.
		for (Partition p : files) {
			cardinality += p.getCardinality();
		}
		return cardinality;
	}

	/**
	 * This method get documents based tweets/hashtags based on the
	 * ServerRequest
	 *
	 * @param indexPath
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	public int GetDocumentsInvertedIndex(String indexPath) throws IOException,
			ParseException {
		int count = 0;
		List<String> documents;
		// Read the documents from inverted index
		KWIndexSearcher indexSearcher = new KWIndexSearcher();
		if (serverRequest.getType().equals(ServerRequest.queryType.tweet)) {
			documents = indexSearcher.search(indexPath,
					KWIndexSearcher.dataType.tweets, serverRequest.getQuery(),
					Integer.MAX_VALUE);
			// SpatialFilter to the documents
			for (String doc : documents) {
				Tweet tweet = new Tweet(doc);
				if (serverRequest.getMbr().insideMBR(
						new Point(tweet.lat, tweet.lon))) {
					// outwriter.write(doc);
					// outwriter.write("\n");
					count++;
//					this.result.put(tweet);
				}
			}
		} else {
			documents = indexSearcher.search(indexPath,
					KWIndexSearcher.dataType.hashtags,
					serverRequest.getQuery(), 5000);
			// SpatialFilter to the documents
			for (String doc : documents) {
				Hashtag hashtag = new Hashtag(doc);
				if (serverRequest.getMbr().insideMBR(
						new Point(hashtag.getLat(), hashtag.getLon()))) {
					// outwriter.write(doc);
					// outwriter.write("\n");
					count++;
				}
			}
		}

		return count;
	}

	/**
	 * This method check if the request for tweets or hashtags
	 *
	 * @param type
	 * @param start
	 * @param end
	 * @return
	 * @throws ParseException
	 */
	private Map<String, String> getIndexArmy() throws ParseException,
			IOException {
		return lookup.getTweetsDayIndex(serverRequest.getStartDate(),
				serverRequest.getEndDate());

	}

	/**
	 * This method check if the dayrequest for tweets or hashtags
	 *
	 * @param type
	 * @param start
	 * @param end
	 * @return
	 * @throws ParseException
	 */
	private Map<String, String> getWeekTree() throws ParseException {
		Map<String, String> result = new HashMap<String, String>();
		Map<Week, String> temp = lookup.getTweetsWeekIndex(
				serverRequest.getStartDate(), serverRequest.getEndDate());
		Iterator<Entry<Week, String>> it = temp.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Week, String> obj = it.next();
			result.put(obj.getKey().toString(), obj.getValue());
		}
		return result;
	}

	private Map<String, String> getMonthTree() throws ParseException,
			IOException {
		List<String> months = lookup.getTweetsMonth(
				serverRequest.getStartDate(), serverRequest.getEndDate());
		Map<String, String> result = new HashMap<String, String>();
		for (String month : months) {
			result.put(month, serverRequest.getRtreeDir()
					+ "/tweets/Month/index." + month);
		}
		return result;

	}

	/***
	 * This method read the master file and return the grid cells
	 * 
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 */
	public GridCell readMastersFile(queryLevel level)
			throws FileNotFoundException, UnsupportedEncodingException,
			IOException, ParseException {
		GridCell cell = new GridCell(this.serverRequest.getMbr(), lookup);

		if (level.equals(queryLevel.Week)) {
			Map<String, String> index = getWeekTree();
			Iterator it = index.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry) it.next();
				// System.out.println("#Start Reading index of " +
				// week.getStart() + "-" + week.getEnd());
				cell.add(
						entry.getKey().toString(),
						readMasterFile(entry.getKey().toString(), entry
								.getValue().toString()));
			}
		} else {
			Map<String, String> indexMonths = getMonthTree();
			System.out
					.println("#number of Months found: " + indexMonths.size());
			Iterator it = indexMonths.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry) it.next();
				// System.out.println("#Start Reading index of " +
				// entry.getKey().toString());
				cell.add(
						entry.getKey().toString(),
						readMasterFile(entry.getKey().toString(), entry
								.getValue().toString()));
			}
		}

		return cell;
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ParseException {
		// args = new String[11];
		// args[0] =
		// "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";//System.getProperty("user.dir")
		// + "/data/result";
		// args[1] = System.getProperty("user.dir")+"/export/";
		// args[2] = "180";//"21.509878763366647";//jedah
		// args[3] = "180";//"39.206080107128436";
		// args[4] = "-180";//"21.473700876235167";
		// args[5] = "-180";//"39.15913072053149";
		// args[6] = "2013-12-01";
		// args[7] = "2013-12-01";
		// args[8] = "tweet";//"tweet";
		// args[9] = "multi";
		// args[10] = "";
		// String folderPath = args[0] + "/";
		// lookup.loadLookupTableToArrayList(folderPath);
		// List<TweetVolumes> result =
		// org.gistic.taghreed.diskBaseQuery.query.DayQueryProcessor.run(args);
		// Collections.sort(result);
		// for (int i = 0; i < result.size(); i++) {
		// System.out.println(result.get(i).dayName + "-" +
		// result.get(i).volume);
		// }
	}
}
