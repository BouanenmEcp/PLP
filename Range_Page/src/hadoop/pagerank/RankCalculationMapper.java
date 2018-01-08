package hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int index1 = value.find("\t");
        int index2 = value.find("\t", index1 + 1);
        
        String page = Text.decode(value.getBytes(), 0, index1);
        String pageRank = Text.decode(value.getBytes(), index1 + 1, index2 - (index1 + 1));
        String liens = Text.decode(value.getBytes(), index2 + 1, value.getLength() - (index2 + 1));
        
        String[] Autrespages = liens.split(",");
        for (String APage : Autrespages) { 
            Text pageRankLiensT = new Text(pageRank + "\t" + Autrespages.length);
            context.write(new Text(APage), pageRankLiensT); 
        }
        
        context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + liens));
        
    }
    
}

