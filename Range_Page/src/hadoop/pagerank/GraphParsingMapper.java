package hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphParsingMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        
        if (value.charAt(0) != '#') {
            
            int Index = value.find("\t");
            String Noeud1 = Text.decode(value.getBytes(), 0, Index);
            String Noeud2 = Text.decode(value.getBytes(), Index + 1, value.getLength() - (Index + 1));
            context.write(new Text(Noeud1), new Text(Noeud2));
            
            PageRank.NODES.add(Noeud1);

            PageRank.NODES.add(Noeud2);
            
        }
 
    }
    
}
