package TreeHeight;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TreesHeightMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String TempString = value.toString();
		String[] SingleBookData = TempString.split(";");
		double maxValuePart = 0.0 ;
		if ( !SingleBookData[6].isEmpty() )  {
			if (!(SingleBookData[6].equals("HAUTEUR"))) {
			    maxValuePart = Double.parseDouble(SingleBookData[6]);
			}
		}
		int maxValuePartint = (int)maxValuePart ;
		output.collect(new Text(SingleBookData[2]), new IntWritable(maxValuePartint));
	}
}