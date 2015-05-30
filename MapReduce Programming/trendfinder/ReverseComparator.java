package com.hadoop.mapReduce.trendfinder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseComparator extends WritableComparator{


	protected ReverseComparator() {
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
 
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text)w1;
		Text k2 = (Text)w2;
		
		return -1 * k1.compareTo(k2);
	}
}
