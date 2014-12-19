package com.hadoop.jackson.keyword;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.jackson.keyword.WordCountAnalyzer.AnalyzerMapper;
import com.hadoop.jackson.keyword.WordCountAnalyzer.CountReducer;

public class JobDefiner {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String [] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length != 3) {
	       System.err.println("Usage: key word analyzer <in> <out1> <out2>");
	       System.exit(2);
	    }
		
		Job job1 = new Job(configuration, "key word analyzer");
		job1.setJarByClass(JobDefiner.class);
	    job1.setMapperClass(AnalyzerMapper.class);
	    job1.setCombinerClass(CountReducer.class);
	    job1.setReducerClass(CountReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	    Path outPath1 = new Path(otherArgs[1]);
	    FileOutputFormat.setOutputPath(job1, outPath1);
	    job1.waitForCompletion(true);
	    
	    
	    Job job2 = new Job(configuration, "result sort");
		job2.setJarByClass(JobDefiner.class);
		job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setMapperClass(SortKeyWordHandler.SortMapper.class);
	    job2.setReducerClass(SortKeyWordHandler.SortReducer.class);
	    // key按照降序排列
	    job2.setSortComparatorClass(DescWritableComparator.class);
	    job2.setPartitionerClass(SortKeyWordHandler.SortPartitioner.class);
	    FileInputFormat.addInputPath(job2, outPath1);
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
	    job2.waitForCompletion(true);

	}

}

