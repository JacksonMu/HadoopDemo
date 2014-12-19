package com.hadoop.jackson.keyword;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class WordCountAnalyzer {
	
	public static class AnalyzerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			breakupSentence(value.toString(), context);
		}
	    
		/**
		 * 用分词器将一段话拆分成多个词。
		 * 分出一个词就将数量置为1。
		 * 
		 * @param sentence
		 * @param context
		 * @throws IOException 
		 * @throws InterruptedException 
		 */
		private void breakupSentence(String sentence, Mapper<Object, Text, Text,
				IntWritable>.Context context) throws IOException, InterruptedException {
			Analyzer analyzer = new IKAnalyzer(true);
			TokenStream tokenStream = analyzer.tokenStream("content",
					new StringReader(sentence));
			tokenStream.addAttribute(CharTermAttribute.class);
			while (tokenStream.incrementToken()) {
				CharTermAttribute charTermAttribute = tokenStream
						.getAttribute(CharTermAttribute.class);
				word.set(charTermAttribute.toString());

				context.write(word, one);
			}
		}
	    
	}
	
	// 统计每个词出现的次数。
	public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();
		
		 @Override
		 protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			  int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
	    }
		
	}

}

