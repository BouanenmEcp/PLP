package cs.Lab2.TfIdf;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;




public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public WordFrequencyMapper() {
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text word = new Text();
	
		String fileName = ((FileSplit) context.getInputSplit()).getPath()
				.getName();

		
		for (String token: value.toString().split("\\s+")) {
			token  = token.toLowerCase(); 
			int l = token.length();
			
			if (token == null || token.isEmpty())  {}
			else {
				
			if (Character.isLetter(token.charAt(0)) ||  Character.isDigit(token.charAt(0)))
			{
				
			if (Character.isLetter(token.charAt(l-1)) == false && Character.isDigit(token.charAt(l-1))==false){
					token = token.substring(0,l-1);
					if (Character.isLetter(token.charAt(l-2)) == false && Character.isDigit(token.charAt(l-2))==false){
						token = token.substring(0,l-2);
						if (Character.isLetter(token.charAt(l-3)) == false && Character.isDigit(token.charAt(l-3))==false){
			
						}
						
					}
					
			}
			word.set(token+ " in " +fileName.toString());
            context.write(word, new IntWritable(1));}
         }
      }
		} 
	}

		
		