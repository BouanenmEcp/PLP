package cs.Lab2.TfIdf;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordsTFIDFReducer extends Reducer<Text, Text, Text, Text> {
	 

		 
	    private static final DecimalFormat DF = new DecimalFormat("###.########");
	 
	    public WordsTFIDFReducer() {
	    }
	    
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        int numbersDoc = Integer.parseInt(context.getJobName());
	        int numbersDocWithWord = 0;
	        Map<String, String> freq = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] freqDocu = val.toString().split("=");
	            numbersDocWithWord++;
	            freq.put(freqDocu[0], freqDocu[1]);
	        }
	        for (String document : freq.keySet()) {
	            String[] OccuNumberTotalWords = freq.get(document).split("/");
	 
	            //calcul de tf
	            double tf = Double.valueOf(Double.valueOf(OccuNumberTotalWords[0])
	                    / Double.valueOf(OccuNumberTotalWords[1]));
	 
	            //calcul de idf
	            double idf = (double) numbersDoc / (double) numbersDocWithWord;
	 
	            //calcul de TfIdf
	            double tfIdf = numbersDoc == numbersDocWithWord ?
	                    tf : tf * Math.log10(idf);

	            context.write(new Text(key + " in " + document), new Text("\t" + DF.format(tfIdf)));
        
	        }
	    }
	}