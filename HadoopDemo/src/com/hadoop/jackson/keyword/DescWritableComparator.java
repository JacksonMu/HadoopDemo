package com.hadoop.jackson.keyword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescWritableComparator extends WritableComparator {

	protected DescWritableComparator() {
		super(IntWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
	
}
