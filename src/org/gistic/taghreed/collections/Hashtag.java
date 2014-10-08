/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.gistic.taghreed.collections;

/**
 *
 * @author turtle
 */
public class Hashtag {
    String lat;
    String lon;
    String hashtagText;

    public Hashtag() {
    }
    
    public Hashtag(String hashtagLine){
        String[] token = hashtagLine.split(",");
        this.lat = token[0];
        this.lon = token[1];
        this.hashtagText = token[2];
    }

    public Hashtag(String lat, String lon, String hashtagText) {
        this.lat = lat;
        this.lon = lon;
        this.hashtagText = hashtagText;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getHashtagText() {
        return hashtagText;
    }

    public void setHashtagText(String hashtagText) {
        this.hashtagText = hashtagText;
    }

    @Override
    public String toString() {
        return  lat + "," + lon + "," + hashtagText;
    }
    
    
    
}
