package org.gistic.taghreed.taqreer;


public class EventSide {
	public String sideName;
	public String sideKeyword;
	public String sideColor;
	
	public EventSide(String sideName, String sideKeyword, String sideColor){
		this.sideName = sideName;
		this.sideKeyword = sideKeyword;
		
		if (sideColor == "") 
			this.sideColor="#C50707";
		else
			this.sideColor = sideColor;
	}
	
	public EventSide(){
		
	}
	

}
