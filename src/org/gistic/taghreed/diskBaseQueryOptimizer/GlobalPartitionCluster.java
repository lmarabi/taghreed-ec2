package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;

import org.gistic.taghreed.Commons;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.gistic.taghreed.diskBaseQuery.query.Lookup;

public class GlobalPartitionCluster {
	private GridCell whole;
	private double threashold;
	
	/**
	 * threashold value between 1.0- 0.0
	 * Example: threashold 90 and above parameter 0.90
	 * Example: threashold 85 and above parameter 0.85
	 * Example: threashold 100 and above parameter 1.0
	 * @param threashold
	 */
	public GlobalPartitionCluster(double threashold) {
		MBR mbr = new MBR(new Point(90, 180), new Point(-90, -180));
	    this.whole = new GridCell(mbr, new Lookup());
	    this.threashold = threashold;
	}
	
	public double getThreasholdConfidence() {
		return threashold*100;
	}
	
	private void CreatespatioTemporalCluster() throws NumberFormatException, IOException, ParseException{
		Commons config = new Commons();
		File master;
		String fileString = config.getTweetFlushDir() + "/inputDataStatistics.txt";
		master = new File(fileString);
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(fileString.replace(".txt", "_Day.cluster"), false), "UTF-8");
		BufferedReader reader = new BufferedReader(new FileReader(master));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split("_");
		  this.whole.daysCardinality.put(temp[0], Long.parseLong(temp[1]));
		}
		
		
		this.whole.initCluster(this.threashold);
		
		writer.write(this.whole.toStringHistogram());
		reader.close();
		writer.close();
	}
	
	

	public static void main(String[] args) throws NumberFormatException, IOException, ParseException {
		GlobalPartitionCluster clusters = new GlobalPartitionCluster(0.90);
		clusters.CreatespatioTemporalCluster();

	}

}
