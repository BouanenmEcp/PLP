package hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        
        int index1 = value.find("\t");
        int index2 = value.find("\t", index1 + 1);
        
        String page = Text.decode(value.getBytes(), 0, index1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), index1 + 1, index2 - (index1 + 1)));
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}
