//WordCountReducer.class

/* (word,1) -> (word,count)
Intput: <Text, 1>
Output: <Text, Inwritable>
*/


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text keyValue, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		//sum the value for special key
		int sum = 0;
		for(IntWritable val : value) {
			sum += val.get();
		}
		context.write(keyValue, new IntWritable(sum));
	}
	
}
