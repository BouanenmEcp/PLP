package cs.Lab2.TfIdf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
        String line=value.toString();
        
        String[] tokens=line.split("\t");

        context.write(new Text(tokens[2]), new Text(tokens[0]));
        
    }
       
}
