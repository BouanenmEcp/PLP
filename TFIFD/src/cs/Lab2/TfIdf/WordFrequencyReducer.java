package cs.Lab2.TfIdf;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  int somme = 0;
      for (IntWritable val : values) {
          somme += val.get();
      }
      context.write(key, new IntWritable(somme));
  }
}
