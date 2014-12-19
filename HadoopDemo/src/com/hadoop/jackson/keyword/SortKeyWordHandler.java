package com.hadoop.jackson.keyword;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class SortKeyWordHandler {
	
	public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
		private final static IntWritable wordCount = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
		    while (tokenizer.hasMoreTokens()) {
		    	String a = tokenizer.nextToken().trim();
		        word.set(a);
		        String b = tokenizer.nextToken().trim();
		        wordCount.set(Integer.valueOf(b));
		        context.write(wordCount, word);
		    }
		}
		
	}
	
	public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		private Text result = new Text();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
		        result.set(val.toString());
		        context.write(result, key);
		    }
		}
		
	}
	
	
	public static class SortPartitioner<K, V> extends Partitioner<K, V> {

		@Override
		public int getPartition(K key, V value, int numReduceTasks) {
			int maxValue = 50;
		    int keySection = 0;
		    // 只有传过来的key值大于maxValue 并且numReduceTasks比如大于1个才需要分区，否则直接返回0
		    if (numReduceTasks > 1 && key.hashCode() < maxValue) {
		        int sectionValue = maxValue / (numReduceTasks - 1);
		        int count = 0;
		        while ((key.hashCode() - sectionValue * count) > sectionValue) {
		            count++;
		        }
		        keySection = numReduceTasks - 1 - count;
		    }
		    return keySection;
		}
		
	}

}

