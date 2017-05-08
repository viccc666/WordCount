//  WordCount.class

package com.marlabs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		Configuration config = new Configuration();
		//instantiate a new MapReduce job whose name is WordCount
		Job job = Job.getInstance(config, "wordCount");

		job.setJarByClass(WordCount.class);
		job.setJobName("WordCount Demo");

		job.setMapperClass(WordCountMapper.class);
		

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
}