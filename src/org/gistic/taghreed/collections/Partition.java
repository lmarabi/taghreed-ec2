/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.collections;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;

/**
 *
 * @author turtle
 */
public class Partition {
    private File partition;
    private MBR area;
    private long cardinality;
    private String Day;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    

    public Partition() {
    }

//    public Partition(File partition, MBR area) {
//        this.partition = partition;
//        this.area = area;
//    }
    
    public Partition(String line,String path, String day){
        String[] temp = line.split(",");
        if (temp.length == 8) {
                Point pointMax = new Point(temp[3], temp[4]);
                Point pointMin = new Point(temp[1], temp[2]);
                this.area = new MBR(pointMax, pointMin);
                this.partition = new File(path +temp[7]);
                this.cardinality = Long.parseLong(temp[5]);
                this.Day = day;
            }
    }

    public File getPartition() {
        return partition;
    }

    public void setPartition(File partition) {
        this.partition = partition;
    }

    public MBR getArea() {
        return area;
    }

    public void setArea(MBR area) {
        this.area = area;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * Cardinality of the partition is the number of rows in this partition
     * @return 
     */
    public long getCardinality() {
        return cardinality;
    }
    
    public String getDay() {
    	Date temp = new Date(this.Day);
		return sdf.format(temp);
	}
    
    public String partitionToWKT(){
		return this.Day + "\tPOLYGON (("
    +this.area.getMax().getLon() + " "+ this.area.getMin().getLat() 
    +", "+ this.area.getMax().getLon() + " "+ this.area.getMax().getLat()
    +", "+ this.area.getMin().getLon() + " "+ this.area.getMax().getLat()
    +", "+ this.area.getMin().getLon() + " "+ this.area.getMin().getLat()
    +", "+ this.area.getMax().getLon() + " "+ this.area.getMin().getLat()
    + "))\t" + this.cardinality;
	}
    
    public static void main(String[] args) throws IOException{
    	String[] index = {"quadtree"};//{"str","quadtree","str+"};
    	
    	for(int i=1;i<6;i++){
    		for(int j=0;j<index.length;j++){
    			readpartitions(i,index[j]);
    		}
    	}
    }
    
    private static void readpartitions(int i,String indexType) throws IOException{
    	File master;
		List<Partition> result = new ArrayList<Partition>();
		String fileString = System.getProperty("user.dir") + "/testbuildindex/cluster/_master_"+i+"."+indexType;
		master = new File(fileString);
		fileString = fileString+".WKT";
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(fileString, false), "UTF-8");
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
				Partition part = new Partition(line, System.getProperty("user.dir"), "2014-10-15");
				writer.write(part.partitionToWKT()+"\n");
				
			}
		}
		reader.close();
		writer.close();
    }
    
    
    
}
