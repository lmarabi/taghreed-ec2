/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.collections;

import java.io.File;
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
    

    public Partition() {
    }

//    public Partition(File partition, MBR area) {
//        this.partition = partition;
//        this.area = area;
//    }
    
    public Partition(String line,String path){
        String[] temp = line.split(",");
        if (temp.length == 7) {
                Point pointMax = new Point(temp[3], temp[4]);
                Point pointMin = new Point(temp[1], temp[2]);
                this.area = new MBR(pointMax, pointMin);
                this.partition = new File(path +temp[5]);
                this.cardinality = Long.parseLong(temp[6]);  
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
    
    
    
    
    
}
