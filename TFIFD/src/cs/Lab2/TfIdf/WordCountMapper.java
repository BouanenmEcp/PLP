package cs.Lab2.TfIdf;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public WordCountMapper() {
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String[] string1 = value.toString().split("\t");
	        String[] string2 = string1[0].split(" in ");
	        context.write (new Text(string2[1]), new Text (string2[0] + "=" + string1[1]));
	    }
         
      }

   
		
		