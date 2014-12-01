package org.gistic.taghreed.diskBaseQueryOptimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


class myThread implements Runnable{
	List<Integer> local; 
	int count;
	public myThread(int count) {
		this.local = new ArrayList<Integer>();
		this.count = count;
	}

	@Override
	public void run() {
		System.out.println("Hello from the Thread");
		for(int i =0 ; i< 1000 ; i++ ){
			this.local.add(this.count);
			System.out.println(this.count);
		}
	}
	
	public List<Integer> getList(){
		this.run();
		return this.local;
	}
	
}

public class ThreadingTest {
	
	public static Collection<Integer> L =  Collections.synchronizedCollection(new ArrayList<Integer>());
	public static List<Integer> list = new ArrayList<Integer>();
	
	
	public static void main(String[] args){
		myThread[] t = new myThread[5];
		
		for(int i=0;i<1;i++){
			t[i] = new myThread(i);
			L.addAll(t[i].getList());
			list.addAll(t[i].getList());
			
		}
		System.out.println("**** Collection ****");
		for(Integer i : L){
			System.out.print(","+i);
		}
		System.out.println("\n**** list ****");
		for(Integer i : list){
			System.out.print(","+i);
		}
		
	}

}

