/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.diskBaseQuery.server;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.ActiveUsers;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.PopularUsers;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.collections.TweetVolumes;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQueryOptimizer.QueryPlanner;
import org.gistic.taghreed.diskBaseQueryOptimizer.QueryPlanner2;

import com.google.gson.stream.JsonWriter;

/**
 *
 * @author turtle
 */
public class HomeServer extends AbstractHandler {
//	static QueryPlanner queryPlanner;
	static QueryPlanner2 queryPlanner;

	@Override
	public void handle(String s, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException, FileNotFoundException,
			UnsupportedEncodingException {

		// String s2 = s;
		// Request baseRequest2 = baseRequest;
		// HttpServletRequest request2 = request;
		// HttpServletResponse response2 =response;
		// Runnable reRunnabl = new RequestThread(counter, s, baseRequest,
		// request, response);
		// executor.execute(reRunnabl);
		// while(!executor.isTerminated()){
		try {
			executeRequest(s, baseRequest, request, response);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//
		// }

	}

	public synchronized void  executeRequest(String s, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException, FileNotFoundException,
			UnsupportedEncodingException, InterruptedException {

		try {

			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("text/html;charset=utf-8");
			response.addHeader("Access-Control-Allow-Origin", "*");
			response.addHeader("Content-Encoding", "gzip");
			response.addHeader("Access-Control-Allow-Credentials", "true");
			baseRequest.setHandled(true);
			String path = request.getPathInfo();

			if (path.equals("/allqueries")) {

				String query = baseRequest.getParameter("q");
				String minLong = baseRequest.getParameter("min_long");
				String minLat = baseRequest.getParameter("min_lat");
				String maxLong = baseRequest.getParameter("max_long");
				String maxLat = baseRequest.getParameter("max_lat");
				String startDate = baseRequest.getParameter("startDate");
				String endDate = baseRequest.getParameter("endDate");
				String topK = baseRequest.getParameter("topk");
				String index = (baseRequest.getParameter("index") == null) ? "Day"
						: baseRequest.getParameter("index");
				String queryParameters = "\nMBR: (" + minLong + "-" + minLat
						+ ") - (" + maxLong + "-" + maxLat + "),StartDate: "
						+ startDate + ",EndDate: " + endDate + ",topK: " + topK
						+ ",Keyword: " + query + ",";
				ServerRequest req = new ServerRequest();
				MBR mbr = new MBR(new Point(maxLat, maxLong), new Point(
						minLat, minLong));
				req.setMBR(mbr);
				req.setQuery(query);
				req.setStartDate(startDate);
				req.setEndDate(endDate);
				List<Tweet> tweetsResult;
				double startTime = System.currentTimeMillis();
				queryLevel plan = this.queryPlanner.getQueryPlan(startDate, endDate, mbr);
				double endTime = System.currentTimeMillis();
				System.err.println("Query Plan Estimation Time: "+(endTime - startTime) + " ms\tSuggested plan:"+plan.toString());
				req.setQueryResolution(plan);
				startTime = System.currentTimeMillis();
				req.getTweetsRtreeDays();
				endTime = System.currentTimeMillis();
				System.err.println("Query Time: "+(endTime - startTime) + " ms");

				startTime = System.currentTimeMillis();
				List<PopularHashtags> popularHashtags;
				popularHashtags = req.getHashtags();

				endTime = System.currentTimeMillis();
				System.out.println("*********\n" + "Found Hashtags: "
						+ popularHashtags.size() + "\n" + "*************");
				

				List<ActiveUsers> activePeople = null;
				startTime = System.currentTimeMillis();
				activePeople = req.getActiveUser();
				endTime = System.currentTimeMillis();
				

				startTime = System.currentTimeMillis();
				List<PopularUsers> popularPeople = null;
				popularPeople = req.getPopularUsers();
				endTime = System.currentTimeMillis();
				

				// List<TweetVolumes> tweetVolumes = null;
				// tweetVolumes = req.getTweetVolumesFast(tweetsResult);

				// Retrive the topK tweets
				startTime = System.currentTimeMillis();
				int topkInt = Integer.parseInt(topK);
				tweetsResult = req.getTopKtweets();
				endTime = System.currentTimeMillis();
				

				JsonWriter writer = new JsonWriter(new OutputStreamWriter(
						new GZIPOutputStream(response.getOutputStream()),
						"UTF-8"));
				writer.setLenient(true);
				writer.beginObject();
				writer.name("tweets");
				writer.beginArray();
				for (Tweet x : tweetsResult) {
					writer.beginObject();
					writer.name("created_at").value(x.created_at);
					writer.name("tweet_id").value(x.tweetID);
					writer.name("user_id").value(x.userID);
					writer.name("screen_name").value(x.screenName);
					writer.name("text").value(x.tweetText);
					writer.name("lang").value(x.language);
					writer.name("os").value(x.osystem);
					writer.name("lat").value(x.lat);
					writer.name("long").value(x.lon);
					writer.endObject();
				}
				writer.endArray();
				// writer.endObject();
				// writer.beginObject();
				writer.name("active_people");
				writer.beginArray();
				if (activePeople.size() > 30) {
					for (int i = 0; i < 30; i++) {
						writer.beginObject();
						writer.name("screen_name").value(
								activePeople.get(i).userName);
						writer.name("tweet_count").value(
								activePeople.get(i).tweetCount);
						writer.name("display_image").value("img/image.jpg");
						writer.endObject();
					}
				} else {
					for (int i = 0; i < activePeople.size(); i++) {
						writer.beginObject();
						writer.name("screen_name").value(
								activePeople.get(i).userName);
						writer.name("tweet_count").value(
								activePeople.get(i).tweetCount);
						writer.name("display_image").value("img/image.jpg");
						writer.endObject();
					}
				}

				writer.endArray();
				// writer.endObject();
				// writer.beginObject();
				writer.name("popular_people");
				writer.beginArray();
				if (popularPeople.size() > 25) {
					for (int i = 0; i < 25; i++) {
						writer.beginObject();
						writer.name("screen_name").value(
								popularPeople.get(i).screenName);
						writer.name("followers_count").value(
								popularPeople.get(i).followersCount);
						writer.name("display_image").value("img/image.jpg");
						writer.endObject();
					}
				} else {
					for (int i = 0; i < popularPeople.size(); i++) {
						writer.beginObject();
						writer.name("screen_name").value(
								popularPeople.get(i).screenName);
						writer.name("followers_count").value(
								popularPeople.get(i).followersCount);
						writer.name("display_image").value("img/image.jpg");
						writer.endObject();
					}

				}
				writer.endArray();
				// writer.endObject();
				// writer.beginObject();
				writer.name("popular_hashtags");
				writer.beginArray();
				if (popularHashtags.size() > 50) {
					for (int i = 0; i < 50; i++) {
						writer.beginObject();
						writer.name("name").value(
								popularHashtags.get(i).hashtagName);
						writer.name("count").value(
								popularHashtags.get(i).hashtagCount);
						writer.endObject();
					}
				} else {
					for (int i = 0; i < popularHashtags.size(); i++) {
						writer.beginObject();
						writer.name("name").value(
								popularHashtags.get(i).hashtagName);
						writer.name("count").value(
								popularHashtags.get(i).hashtagCount);
						writer.endObject();
					}
				}
				writer.endArray();
				// writer.endObject();
				// writer.beginObject();
				writer.name("day_volume");
				writer.beginArray();
				for (TweetVolumes vol : req.getDayVolumes()) {
					writer.beginObject();
					writer.name("day").value(vol.dayName);
					writer.name("tweet_count").value(vol.volume);
					writer.endObject();
				}
				writer.endArray();
				writer.endObject();
				writer.close();

			} else {
				response.getWriter()
						.print("<h2> Welcome to this tutorial </h2> <br /><br />"
								+ "These are the main functionalties that are implemeneted in this server <br /><br />"
								+ "1 - Key word Search : (input : MBR + query) <br /><br />"
								+ "<a href='http://10.14.22.10:8085/keywordSearch?q=dubai&max_lat=180&max_long=180&min_lat=-180&min_long=-180'> http://10.14.22.10:8085/keywordSearch?q=dubai&max_lat=180&max_long=180&min_lat=-180&min_long=-180 </a> <br /><br />"
								+ "2- Temporal Search (input : Start Date + End Date + MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/temporalSearch?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/temporalSearch?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "3- Spatial Search (input : MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/spatialSearch?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/spatialSearch?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "4- Tweets per day (input : Start Date + End Date + MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/dayVolume?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/dayVolume?startDate=2013-10-12&endDate=2013-10-15&max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "5- Hashtag Count (input : MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/hashtagcount?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/hashtagcount?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "6- Popular People (input : MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/popularPerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/popularPerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "7- Active users (input : MBR) <br / ><br />"
								+ "<a href='http://10.14.22.10:8085/activePerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180'>http://10.14.22.10:8085/activePerson?max_lat=180&max_long=180&min_lat=-180&min_long=-180</a><br /><br />"
								+ "<h1> Thank You </h1>");
			}

		} catch (ParseException e) {
			System.out.println("error happened");
		}

	}

	public static void main(String[] args) throws Exception {
		queryPlanner = new QueryPlanner2();
		Server server = new Server(8085);
		server.setHandler(new HomeServer());
		server.start();
		server.join();
	}

}
