package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.collections.Partition;

public class HistogramCluster {
	List<Partition> histogram;

	public HistogramCluster(String path) throws IOException {
		File master;
		this.histogram = new ArrayList<Partition>();
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
		String line = null;
		while ((line = reader.readLine()) != null) {
			Partition part = new Partition(line, path, "");
			this.histogram.add(part);
		}
		reader.close();
	}
	
	
	/**
	 * This method return the volume of the whole histogram, for instance the number of tweets in the index
	 * @return long volume of the records in the index.
	 */
	public long getHistogramVolume(){
		long result =0 ; 
		for(Partition part : histogram){
			result += part.getCardinality();
		}
		return result;
	}
	
	
	/**
	 * This method return the cardinality of given query
	 * @param queryMbr
	 * @return
	 */
	public long getCardinality(MBR queryMbr){
		long cardinality = 0;
		for(Partition part : histogram){
			if(queryMbr.Intersect(part.getArea())){
				cardinality += part.getCardinality();
			}
		}
		return cardinality;
	}
	
	/**
	 * Get the max latitude in the partition. 
	 * @return
	 */
	public double getMaxlat(){
		double tempDouble,result = 0;
		for(Partition part : histogram){
			tempDouble = part.getArea().getMax().getLat(); 
			if(tempDouble > result){
				result = tempDouble;
			}
		}
		return result;
	}
	
	/**
	 * Get the max Longitude in the partition. 
	 * @return
	 */
	public double getMaxLon(){
		double tempDouble,result = 0;
		for(Partition part : histogram){
			tempDouble = part.getArea().getMax().getLon();
			if(tempDouble > result){
				result = tempDouble;
			}
		}
		return result;
	}
	
	
	/**
	 * Get the min latitude in the partition. 
	 * @return
	 */
	public double getMinLat(){
		double tempDouble;
		double result = Double.MAX_VALUE;
		for(Partition part : histogram){
			tempDouble = part.getArea().getMin().getLat();
			if(tempDouble < result){
				result = tempDouble;
			}
		}
		return result;
	}
	
	
	/**
	 * Get the min latitude in the partition. 
	 * @return
	 */
	public double getMinLon(){
		double tempDouble;
		double result = Double.MAX_VALUE;
		for(Partition part : histogram){
			tempDouble = part.getArea().getMin().getLon();
			if(tempDouble < result){
				result = tempDouble;
			}
		}
		return result;
	}
	
	
	
	public void printHistogram(){
		for(Partition p : histogram){
			System.out.println(p.getArea().toWKT());
		}
	}

}
