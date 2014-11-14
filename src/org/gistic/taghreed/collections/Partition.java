/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.collections;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

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
    
    
    
    
    
}
