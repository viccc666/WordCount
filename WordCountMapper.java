// WordCountMapper.class

/* (LinNumber, line) -> (word, 1)
Mapper input key: LongWritable
Mapper input value: Text
Mapper output key: LongWritable
Mapper output value: Text
*/


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable KeyValue, Text value, Context context) throws IOException, InterruptedException {
		//remove all numerical values or punctuation in source file
		String clean_line = value.toString().replaceAll("[^a-zA-Z\\s]","");

		//split the line into words separated by white space
		String[] result = clean_line.split("\\s");
		for(int n = 0; n < result.length; n++) {
			context.write(new Text(result[n]), new IntWritable(1));
		}
	}
}