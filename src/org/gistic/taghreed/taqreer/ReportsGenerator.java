package org.gistic.taghreed.taqreer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;






import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class ReportsGenerator {
	public enum SearchType {
		HashTag, Keyword
	}

	private final String folder = "report_files";
	private final String fileExt = "trcf";

	private String reportName;

	DateTime minDate;
	DateTime maxDate;
	MBR reportRegion;
	List<Area> areas = new ArrayList<Area>();
	SearchType searchType;
	List<EventSide> eventSides = new ArrayList<EventSide>();

	private int intervalDuration;
	private int intervalsCount;
	private Map<EventSide, List<Tweet>> sidesTweets = new HashMap<EventSide, List<Tweet>>();
	private Map<String, List<Tweet>> analyzedTweets = new TreeMap<String, List<Tweet>>();

	public void setReportName(String reportName) {
		this.reportName = folder + File.separator + reportName + "." + fileExt;
	}

	public String getReportName() {
		return reportName;
	}

	// use this method after setting all initial prameters (minDate, maxDate,
	// ....) to generate the report file
	public void generateReport() throws MalformedURLException,
			IOException, ParseException, URISyntaxException {
		// 1- calcualte the interval
		// 2- fetch tweets for each side
		// 3- tweet spatio-temporal analysis
		// 4- prepare configration file

		calculateIntervalDuration();
		fetchTweets();
		spatioTemportalAnalysis();
		makeConfigurationFile();

	}

	// use this method after setting all initial prameters (minDate, maxDate,
	// ....)
	// it will send event after each stage to the server, in this way progress
	// can be displayed to the client
	public void generateReportWithEvents(Writer writer)
			throws MalformedURLException, IOException, ParseException,
			URISyntaxException {

		/*
		 * interface will recognize three events: 1- "status_changed" data
		 * represent the stage and the percentage: -- 0.25 Fetching Your Tweets
		 * -- 0.50 Spatioa Temporal Analysing For Your Tweets -- 0.75 Building
		 * Configuration File -- client always will have a stage at the end, so
		 * here percentage is calculated by as 4 stages
		 * 
		 * 2- "process_complete" -- so client can be informed to take its next
		 * step
		 * 
		 * 3- "error" -- on errors
		 */

		// data will be either : 0.20fetchTweets

		calculateIntervalDuration();

		// fetching tweets
		writer.write("event:status_changed\n");
		writer.write("data: " + "0.25 Fetching Your Tweets" + "\n\n");
		writer.flush();
		fetchTweets();

		// analysis
		writer.write("event:status_changed\n");
		writer.write("data: "
				+ "0.50 Spatioa Temporal Analysing For Your Tweets" + "\n\n");
		writer.flush();
		spatioTemportalAnalysis();

		// configuration file
		writer.write("event:status_changed\n");
		writer.write("data: " + "0.75 Building Configuration File" + "\n\n");
		writer.flush();
		makeConfigurationFile();

		writer.write("event:process_complete\n");
		writer.write("data: " + reportName + "\n\n");
		writer.flush();

	}

	public void calculateIntervalDuration() {
		Duration duration = new Duration(minDate, maxDate.plusDays(1));
		long hours = duration.getStandardHours();

		if ((hours / 1) <= 144)
			intervalDuration = 1;

		else if ((hours / 2) <= 144)
			intervalDuration = 2;

		else if ((hours / 3) <= 144)
			intervalDuration = 3;

		else if ((hours / 4) <= 144)
			intervalDuration = 4;

		else if ((hours / 6) <= 144)
			intervalDuration = 6;

		else if ((hours / 8) <= 144)
			intervalDuration = 8;

		else if ((hours / 12) <= 144)
			intervalDuration = 12;

		else if ((hours / 24) <= 144)
			intervalDuration = 24;

		else
			intervalDuration = 168;

		// finding the total number of inervals
		intervalsCount = (int) Math.ceil(hours / intervalDuration);
	}

	public void fetchTweets() throws IOException, MalformedURLException,
			ParseException, URISyntaxException {
		//String server = "10.10.10.51";
		 String server = "localhost";
		int port = 8085;
		DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-M-d");

		if (eventSides.size() == 0) {
			eventSides.add(new EventSide("Default", "", ""));
		}
		
		// loop through event sides and fetch the tweets

		for (EventSide side : eventSides) {
			String url = "http://" + server + ":" + port
					+ "/allqueries?max_lat=" + reportRegion.getMax().getLat()
					+ "&max_long=" + reportRegion.getMax().getLon() + "&min_lat="
					+ reportRegion.getMin().getLat() + "&min_long=" + reportRegion.getMin().getLon()
					+ "&startDate=" + formatter.print(minDate) + "&endDate="
					+ formatter.print(maxDate) + "&q="
					+ URLEncoder.encode((side.sideKeyword), "UTF-8") + "&"
					+ "topk=" + "999999999" + "&index=DAY";

			InputStream response = sendURL(url);
			System.out.println("Parsing JSON Response...");
			List<Tweet> responseTweets = parseTweetsJsonStream(response);
			sidesTweets.put(side, responseTweets);

		}

		System.out.println("fetching tweets done");
		for (EventSide side : sidesTweets.keySet()) {
			System.out.println(side.sideName + "-" + side.sideKeyword + ":"
					+ sidesTweets.get(side).size() + " Tweet");
		}
	}

	public InputStream sendURL(String url) throws MalformedURLException,
			IOException {
		URL con = new URL(url);

		System.out.println("Sending Rquest\n" + url);
		InputStream in = con.openConnection().getInputStream();
		return in;

	}

    public List<Tweet> getTweets(ServerRequest req)
    {
        return null;
    }

	public List<Tweet> parseTweetsJsonStream(InputStream in)
			throws UnsupportedEncodingException, IOException, ParseException {
		List<Tweet> result = new ArrayList<Tweet>();

		GZIPInputStream gis = new GZIPInputStream(in);
		JsonReader reader = new JsonReader(new InputStreamReader(gis, "UTF-8"));

		// response is like {"tweets":[{"created_at:],...}
		// begin {
		reader.beginObject(); // the main object of the response

		// "tweets"
		reader.nextName();

		// [
		reader.beginArray();

		while (reader.peek() == JsonToken.BEGIN_OBJECT) {
			Tweet tweet = new Tweet();
			// {
			reader.beginObject();

			while (reader.peek() != JsonToken.END_OBJECT) {
				if(reader.nextName().equals("created_at")) {
                                    SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					tweet.createdAt = sdf.parse(reader.nextString());
                                }else if(reader.nextName().equals("tweet_id")){
					tweet.tweetId = reader.nextLong();
                                }else if(reader.nextName().equals("user_id")){
					tweet.userId = reader.nextLong();
                                }else if(reader.nextName().equals("screen_name")){
					tweet.screenName = reader.nextString();
                                }else if(reader.nextName().equals("text")){
					tweet.text = reader.nextString();
                                }else if(reader.nextName().equals("lat")){
					tweet.latitude = reader.nextDouble();
                                }else if(reader.nextName().equals("long")){
					tweet.longitude = reader.nextDouble();
				}
			}

			reader.endObject();
			result.add(tweet);
		}

		reader.close();
		return result;
	}




	public void spatioTemportalAnalysis() {

		// add other area
		areas.add(new Area("Other", reportRegion));

		// create groups for each side each area each intervale
		// i.e. side0_area1_intrv_5 will contain tweets of side 0 which are in
		// area 1 during the time interval number 5
		// initially will creat the groups only, tweets will be added later

		for (int s = 0; s < eventSides.size(); s++) {
			for (int a = 0; a < areas.size(); a++) {
				for (int i = 0; i < intervalsCount; i++) {
					analyzedTweets.put("side" + s + "_" + "area" + a + "_"
							+ "intrv" + i, new ArrayList<Tweet>());
				}
			}

		}

		// check all tweets and put each one in its coressponding set.
		for (int s = 0; s < eventSides.size(); s++) {
			EventSide side = eventSides.get(s);
			for (Tweet tweet : sidesTweets.get(side)) {

				// calculating interval number
				int intrv = (int) (new DateTime(tweet.createdAt).getMillis() - minDate
						.getMillis()) / (1000 * 60 * 60 * intervalDuration);

				// added to fix some tweets time like 24:05:12
				// I handled them as they are on the next day, so they are out
				// of the analysis
				if (intrv >= intervalsCount)
					continue;

				// finding area

				// if single area the it is other area and every thing will be
				// in it
				// and no need for each tweet to be checked wheather it is on
				// other area or not.
				int tweetArea = areas.size() - 1; // default is other area which
													// is the last item added to
													// the list

				if (areas.size() == 1)
					tweetArea = 0;
				// else loop through all other areas
				for (int a = 0; a < areas.size(); a++) {
					Area myArea = areas.get(a);
					if (myArea.mbr.insideMBR(new Point(tweet.longitude, tweet.latitude))){
						tweetArea = a;
						break;
					}
				}

				analyzedTweets.get(
						"side" + s + "_" + "area" + tweetArea + "_" + "intrv"
								+ intrv).add(tweet);
			}
		}
	}

	private void makeConfigurationFile() throws IOException {
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter(  new FileOutputStream(new File(reportName)),"UTF-8"));
		
		//the global variable to hold the report in the interface
		String holder = "myReport";
		


		// writing interval duration
		writer.write(holder + ".intervalDuration=" + intervalDuration + ";\n");
		
		
		//writing report dates
		writer.write(holder + ".minDate=" + String.format("new Date(%s, %s, %s, 0, 0, 0)", 
															minDate.year().get(), minDate.monthOfYear().get()-1, minDate.dayOfMonth().get()) + ";\n");
		
		writer.write(holder + ".maxDate=" + String.format("new Date(%s, %s, %s, 0, 0, 0)", 
				maxDate.year().get(), maxDate.monthOfYear().get()-1, maxDate.dayOfMonth().get()) + ";\n");
		
		//writing report region
		writer.write(holder + ".reportRegion={\n" + "minLat:" + reportRegion.getMin().getLat() + ",\n" +
													"minLong:" + reportRegion.getMin().getLon() + ",\n" +
													"maxLat:" + reportRegion.getMax().getLat() + ",\n" +
													"maxLong:" + reportRegion.getMax().getLon() + "};\n");
				
		
		

		// writing areas
		// start the array
		writer.write(holder + ".areas=[\n");
		for (int i = 0; i < areas.size(); i++) {
			Area myArea = areas.get(i);
			writer.write("\"" + myArea.areaName + "\"");

			// print comma if it is not the last element
			if (i < (areas.size() - 1))
				writer.write(",");
		}

		// end the array
		writer.write("];\n");

		// writing sides 
		// start the array
		writer.write(holder + ".eventSides=[\n");
		for (int i = 0; i < eventSides.size(); i++) {
			EventSide mySide = eventSides.get(i);
			//writing object
			writer.write("{" + "sideName:" + "\"" + mySide.sideName + "\",\n");
			writer.write("sideColor:" + "\"" + mySide.sideColor + "\"}\n");

			// print comma if it is not the last element
			if (i < (eventSides.size() - 1))
				writer.write(",");
		}

		// end the array
		writer.write("];\n");
		
		
		// loop throug all groups, each group represent an array with its tweets
		// writing will be in javascript format

		for (String key : analyzedTweets.keySet()) {
			List<Tweet> myTweets = analyzedTweets.get(key);

			// starting the array
			writer.write(holder + "." + key + " = [\n");

			for (int i = 0; i < myTweets.size(); i++) {
				Tweet myTweet = myTweets.get(i);

				// adding tweet to the array
				writer.write(String.format("new Tweet(\"%s\", %s, %s, \"%s\")",
						myTweet.tweetId, myTweet.longitude, myTweet.latitude,
						myTweet.screenName));
				if (i < (myTweets.size() - 1))
					writer.write(",\n");
			}

			// closing the array
			writer.write("\n];\n");

		}

		writer.close();
		System.out.println("Finish writing the configuration file");
	}

	public static void main(String[] args) {
		ReportsGenerator x = new ReportsGenerator();
		x.minDate = new DateTime(2014, 5, 1, 0, 0);
		x.maxDate = new DateTime(2014, 5, 7, 0, 0);
		x.reportRegion = new MBR(new Point(35.54442225195314, 15.3047028774414),
				new Point(45.08000574804689, 28.534729122558588)

        );
		// x.spatialArea = new MapArea(20,20,40,30);
		x.searchType = SearchType.Keyword;
		x.eventSides.add(new EventSide("Hilal", "#الشعب_السعودي_الشقيق", ""));
		// x.eventSides.add(new EventSide("Nasr", "النصر"));
		try {
			x.generateReport();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}

	}

}
