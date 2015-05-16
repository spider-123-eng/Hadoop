package com.hadoop.mapReduce.logfiles;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DateWritable implements WritableComparable<DateWritable>
{
	private final static SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd' T 'HH:mm:ss.SSS" );
	private Date date;
	
	public Date getDate()
	{
		return date;
	}
	
	public void setDate( Date date )
	{
		this.date = date;
	}
	
	public void readFields( DataInput in ) throws IOException 
	{
		date = new Date( in.readLong() );
	}
	
	public void write( DataOutput out ) throws IOException 
	{
		out.writeLong( date.getTime() );
	}
	
	public String toString() 
	{
		return formatter.format( date);
	}

    public int compareTo( DateWritable other )
    {
        return date.compareTo( other.getDate() );
    }
}