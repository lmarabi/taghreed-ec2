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
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author turtle
 */
public class PyramidWeek {
    private List<File> files = new ArrayList<File>();
    private int weeknumber;
    private int month;
    private int year;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public PyramidWeek() {
    }
    

    public PyramidWeek(String date) throws ParseException {
        Calendar c = Calendar.getInstance();
        Date day = dateFormat.parse(date);
        c.setTime(day);
        this.month = c.get(Calendar.MONTH);
        this.weeknumber = c.get(Calendar.WEEK_OF_MONTH);
        this.year = c.get(Calendar.YEAR);
    }

    public List<File> getFiles() {
        return files;
    }

    public void setFiles(List<File> files) {
        this.files = files;
    }

    public int getWeeknumber() {
        return weeknumber;
    }

    public void setWeeknumber(int weeknumber) {
        this.weeknumber = weeknumber;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }
    
    public void addNewFile(File file){
        this.files.add(file);
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getYear() {
        return year;
    }
    
    public String getFirstDayOfWeek(){
        return this.files.size() != -1 ? this.files.get(0).getName() : "null";
    }
    
    public String getLastDayofWeek(){
        return this.files.size() > 0 ? this.files.get(this.files.size()-1).getName() : this.files.get(0).getName();
    }
    
    public boolean isTheSameWeek(String date) throws ParseException{
        Calendar c = Calendar.getInstance();
        Date day = dateFormat.parse(date);
        c.setTime(day);
         if(this.month == c.get(Calendar.MONTH) && this.weeknumber ==
                 c.get(Calendar.WEEK_OF_MONTH) &&
                 this.year == c.get(Calendar.YEAR)){
             return true;
         }
        return false;
    }
    
    public HashMap<String, List<File>> buildWeekIndex(List<File> list) throws ParseException{
    	Calendar c = Calendar.getInstance();
    	HashMap<String, List<File>> index = new HashMap<String, List<File>>();
    	for(File file : list){
            Date day = dateFormat.parse(file.getName().replace(".bz2", ""));
            c.setTime(day);
            String hashkey =  c.get(Calendar.YEAR)+"-"+(c.get(Calendar.MONTH)+1)+"-"+c.get(Calendar.WEEK_OF_MONTH);
            if(!index.containsKey(hashkey)){
            	List<File> templist = new ArrayList<File>();
            	templist.add(file);
            	index.put(hashkey, templist);
            }else{
            	List<File> templist = index.get(hashkey);
            	templist.add(file);
            	index.put(hashkey, templist);
            }
    	}
    	return index;
    }
    
    public static void main(String[] arg) throws ParseException{
//    	PyramidWeek obj = new PyramidWeek();
//    	String[] list = new String[30];
//    	for(int i=0;i < 30;i++){
//    		list[i] = "2014-05-"+(i+1);
//    	}
//    	
//    	for(int i=0;i < 31;i++){
//    		obj = new PyramidWeek(list[i]);
//    		for(int j =i ; j< 31; j++){
//    			if(obj.isTheSameWeek(list[j])){
//    				System.out.println(list[j] + " - week"+ obj.weeknumber);
//    			}else{
//    				i=j;
//    				break;
//    			}
//    		}
//    	}
    	
    }
}
