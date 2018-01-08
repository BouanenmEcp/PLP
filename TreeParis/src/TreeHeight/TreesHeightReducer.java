package TreeHeight;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TreesHeightReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {


	@Override
	public void reduce(Text _key,
			Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output, 
			Reporter reporter)
			throws IOException {
		Text key = _key;
		int frequencyForType = 0;
		while (values.hasNext()) {
			
			IntWritable value = (IntWritable) values.next();
			if (value.get() > frequencyForType) {
				frequencyForType = value.get();
			}
			
		}
		output.collect(key, new IntWritable(frequencyForType));
	}
}