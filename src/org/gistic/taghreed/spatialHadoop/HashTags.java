package org.gistic.taghreed.spatialHadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;

public class HashTags extends Point{
	private static final Log LOG = LogFactory.getLog(HashTags.class);
//	public double lat;
//	public double lon;
	public String date;
	public String hashTag;
	
	@Override
	public void write(DataOutput out) throws IOException {
//		out.writeDouble(lat);
//		out.writeDouble(lon);
		super.write(out);
		out.writeUTF(hashTag);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
//		lat = in.readDouble();
//		lon = in.readDouble();
		super.readFields(in);
		hashTag = in.readUTF();
		
	}
	
	@Override
	public Text toText(Text text) {
		byte[] separator = new String(",").getBytes();
//		byte[] latbytes = new Double(lat).toString().getBytes();
//		byte[] lonbytes = new Double(lon).toString().getBytes();
//		text.append(latbytes, 0, latbytes.length);
//	    text.append(separator, 0,separator.length);
//	    text.append(lonbytes, 0, lonbytes.length);
		text.append(date.getBytes(), 0, date.getBytes().length);
		text.append(separator, 0,separator.length);
		super.toText(text);
	    text.append(separator, 0,separator.length);
	    text.append(hashTag.getBytes(), 0, hashTag.getBytes().length);
	    
		return text;
	}
	
	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		date = list[0];
		hashTag = list[3];
		super.fromText(new Text(list[1]+","+list[2]));
	}
	
	@Override
	public HashTags clone() {
		HashTags c = new HashTags();

		c.date = this.date;
		c.hashTag = this.hashTag;
		c.set(x,y);
		return c;
	}

}
