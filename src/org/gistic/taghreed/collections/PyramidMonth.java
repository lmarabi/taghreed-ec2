/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gistic.taghreed.collections;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author turtle
 */
public class PyramidMonth {
	private List<File> files = new ArrayList<File>();
    private int year;
    private int month;
    private int key;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public PyramidMonth() {
    }

    public PyramidMonth(String date) throws ParseException {
         Calendar c = Calendar.getInstance();
        Date day = dateFormat.parse(date);
        c.setTime(day);
       this.year = c.get(Calendar.YEAR);
       this.month = c.get(Calendar.MONTH);
       this.key = this.year+this.month;
    }
    
    public int getKey(){
    	return this.getKey();
    }
    
    public void addFile(File file){
    	this.files.add(file);
    }
    
    public List<File> getFiles(){
    	return this.files;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getYear() {
        return year;
    }
    
    public void setMonth(int month){
    	this.month = month;
    }

	public int getMonth() {
		return month;
	}
    
}
