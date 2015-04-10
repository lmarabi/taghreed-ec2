package umn.ec2.exp;

import java.util.ArrayList;

import java.util.*;

//An interface to be implemented by everyone interested in "Hello" events
interface TimeListener {
 public void reportTime(String time);
}

//Someone who says "Hello"
public class Initiater {
 List<TimeListener> listeners = new ArrayList<TimeListener>();

 public void addListener(TimeListener toAdd) {
     listeners.add(toAdd);
 }

 public void notifyExecutionTime(String time) {
     // Notify everybody that may be interested.
     for (TimeListener hl : listeners)
         hl.reportTime(time);
 }
}