package com.hadoop.mapReduce.Custom.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WebLogWritable implements WritableComparable<WebLogWritable> {

	private Text siteURL, reqDate, timestamp, ipaddress;
	private IntWritable reqNo;

	// Default Constructor
	public WebLogWritable() {
		this.siteURL = new Text();
		this.reqDate = new Text();
		this.timestamp = new Text();
		this.ipaddress = new Text();
		this.reqNo = new IntWritable();
	}

	// Custom Constructor
	public WebLogWritable(IntWritable reqno, Text url, Text rdate, Text rtime,
			Text rip) {
		this.siteURL = url;
		this.reqDate = rdate;
		this.timestamp = rtime;
		this.ipaddress = rip;
		this.reqNo = reqno;
	}

	// Setter method to set the values of WebLogWritable object
	public void set(IntWritable reqno, Text url, Text rdate, Text rtime,
			Text rip) {
		this.siteURL = url;
		this.reqDate = rdate;
		this.timestamp = rtime;
		this.ipaddress = rip;
		this.reqNo = reqno;
	}

	// to get IP address from WebLog Record
	public Text getIp() {
		return ipaddress;
	}

	@Override
	// overriding default readFields method.
	// It de-serializes the byte stream data
	public void readFields(DataInput in) throws IOException {
		ipaddress.readFields(in);
		timestamp.readFields(in);
		reqDate.readFields(in);
		reqNo.readFields(in);
		siteURL.readFields(in);
	}

	@Override
	// It serializes object data into byte stream data
	public void write(DataOutput out) throws IOException {
		ipaddress.write(out);
		timestamp.write(out);
		reqDate.write(out);
		reqNo.write(out);
		siteURL.write(out);
	}

	@Override
	public int compareTo(WebLogWritable o) {
		if (ipaddress.compareTo(o.ipaddress) == 0) {
			return (timestamp.compareTo(o.timestamp));
		} else
			return (ipaddress.compareTo(o.ipaddress));
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof WebLogWritable) {
			WebLogWritable other = (WebLogWritable) o;
			return ipaddress.equals(other.ipaddress)
					&& timestamp.equals(other.timestamp);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return ipaddress.hashCode();
	}
}
