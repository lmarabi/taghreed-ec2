package org.gistic.taghreed.diskBaseQuery.server;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.collections.PopularHashtags;
import org.gistic.taghreed.collections.Tweet;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryIndex;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryLevel;
import org.gistic.taghreed.diskBaseQuery.server.ServerRequest.queryType;
import org.gistic.taghreed.diskBaseQueryOptimizer.QueryPlanner2;
import org.gistic.taghreed.diskBaseQueryOptimizer.TraditionalMultiHistogram;

public class TestErrorHistogram {

	public static void main(String[] args) throws FileNotFoundException,
			UnsupportedEncodingException, IOException, ParseException,
			InterruptedException {
		List<PopularHashtags> popularHashtags = new ArrayList<PopularHashtags>();
		List<Tweet> tweets = new ArrayList<Tweet>();
		ServerRequest req = new ServerRequest();
		req.setType(queryType.tweet);
		req.setIndex(queryIndex.rtree);
		MBR mbr = new MBR(new Point(40.694961541009995, 118.07045041992582),
				new Point(38.98904106170265, 114.92561399414794));
		req.setMBR(mbr);

		// Histogram estimation
		QueryPlanner2 queryPlan = new QueryPlanner2();
		TraditionalMultiHistogram queryPlanTradi = new TraditionalMultiHistogram();
		queryLevel queryEstimated, queryEstimatedTraditional;
		// Writer init
		String fileString = System.getProperty("user.dir") + "/stat.csv";
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(fileString, false), "UTF-8");
		writer.write("queryMBR\tstartDate\tendDate\tExactQueryPlan_Traditional\tHistogramTime_Traditional\tExactExecutionTime_Traditional\tnewIdeaQueryPlan\tHistogramTime\tnewIdeaExecutiontime\tMatchFlag\n");
		// Test
		long startTime, endTime;
		long queryEst_Time, queryEstTradi_Time, queryExec_time, QueryExecTradi_time;
		for (int i = 5; i < 7; i++) {
			for (int j = 1; j < 30; j++) {
				req.setStartDate("2014-05" + "-01");
				req.setEndDate("2014-0" + i + "-" + j);
				// Get the queryPlan
				startTime = System.currentTimeMillis();
				queryEstimated = queryPlan.getQueryPlan(req.getStartDate(),
						req.getEndDate(), req.getMbr());
				endTime = System.currentTimeMillis();
				queryEst_Time = endTime - startTime;
				// Estimate Traditional
				startTime = System.currentTimeMillis();
				queryEstimatedTraditional = queryPlanTradi.getQueryPlan(
						req.getStartDate(), req.getEndDate(), req.getMbr());
				endTime = System.currentTimeMillis();
				queryEstTradi_Time = endTime - startTime;

				// Execute the query
				req.setQueryResolution(queryEstimatedTraditional);
				startTime = System.currentTimeMillis();
				req.getTweetsRtreeDays();
				endTime = System.currentTimeMillis();
				QueryExecTradi_time = endTime - startTime;

				req.setQueryResolution(queryEstimated);
				startTime = System.currentTimeMillis();
				req.getTweetsRtreeDays();
				endTime = System.currentTimeMillis();
				queryExec_time = endTime - startTime;

				String temp = mbr.toWKT() + "\t" + req.getStartDate() + "\t"
						+ req.getEndDate() + "\t" + queryEstimatedTraditional
						+ "\t" + queryEstTradi_Time + "\t"
						+ QueryExecTradi_time + "\t" + queryEstimated + "\t"
						+ queryEst_Time + "\t" + queryExec_time + "\t"
						+ Match(queryEstimated, queryEstimatedTraditional)
						+ "\n";
				System.err.println(temp);
				writer.write(temp);
			}
		}

		writer.close();

	}

	private static String Match(queryLevel queryEstimated,
			queryLevel queryEstimatedTraditional) {
		if (queryEstimated.equals(queryEstimatedTraditional))
			return "true";
		else
			return "false";
	}

}
