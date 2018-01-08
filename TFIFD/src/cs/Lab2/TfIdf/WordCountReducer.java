package cs.Lab2.TfIdf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  int wordCount = 0;
      Map<String, Integer> WordOccurency = new HashMap<String, Integer>();
      for (Text val : values) {
          String[] ch = val.toString().split("=");
          WordOccurency.put(ch[0], Integer.valueOf(ch[1]));
          wordCount += Integer.parseInt(val.toString().split("=")[1]);
      }
      for (String word : WordOccurency.keySet()) {
          context.write(new Text(word + " in " + key.toString()), new Text(WordOccurency.get(word) + "/"
                  + wordCount));
      }
  }

  
}
