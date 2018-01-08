package cs.Lab2.TfIdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF {
	
    public static String input = "input/";
    public static String output = "output";
    
    public static void main(String[] args) throws Exception {
    	
        
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(TFIDF.output)))
            fs.delete(new Path(TFIDF.output), true);
        
        TFIDF TFIDF = new TFIDF(); 
        
        System.out.println("Running Job#1");
        boolean isCompleted = TFIDF.job1(input, output + "/step00");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("Running Job#2");
        isCompleted = TFIDF.job2(output + "/step00", output + "/step01");
        if (!isCompleted) {
            System.exit(1);
        }
                
        System.out.println("Running Job#3");
        isCompleted = TFIDF.job3(output + "/step01", output + "/step02");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("Running Job#4");
        isCompleted = TFIDF.job4(output + "/step02", output + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("DONE!");
        System.exit(0);
        
    }
    
	 
    // job which counts the number of words in each of the documents in the input directory.
	    public boolean job1(String input, String output) throws Exception {
	    	
	       /*Map:
	            Input: (document, each line contents)
	            Output: (word in document, 1))
	        Reducer
	            n = sum of the values of for each key “word@document”
	            Output: ((word in document), n)*/

	 	    	Job job = Job.getInstance(new Configuration(), "Job #1");
	 
	    	 job.setJarByClass(TFIDF.class);
	        job.setMapperClass(WordFrequencyMapper.class);
	        job.setReducerClass(WordFrequencyReducer.class);
	        job.setCombinerClass(WordFrequencyReducer.class);
	 
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	 
	        FileInputFormat.addInputPath(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));
	 
	        return job.waitForCompletion(true);
	        
	        
	        
	    }
	    
	 // job which counts the total number of words for each document, in a way to compare each word with the total number of words.
	    public boolean job2(String input, String output) throws Exception {
	    		        
	   /*Map:
	        Input: ((word@document), n)
	        Re-arrange the mapper to have the key based on each document
	        Output: (document, word=n)
	    Reducer
	        N = totalWordsInDoc = sum [word=n]) for each document
	        Output: ((word in document), (n/N))*/
	    	
	    	Job job = Job.getInstance(new Configuration(), "Job #2");
	        	 
	        job.setJarByClass(TFIDF.class);
	        job.setMapperClass(WordCountMapper.class);
	        job.setReducerClass(WordCountReducer.class);
	 
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	 
	        FileInputFormat.addInputPath(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));
	 
	        return job.waitForCompletion(true);
	    }
	    
	  // job which counts the number of documents in which a “term” appears in each document in corpus and calculates the TF-IDF
	    public boolean job3(String input, String output) throws Exception {
	    	
    /* Map:
        Input: ((term in document), n/N)
        Re-arrange the mapper to have the word as the key, since we need to count the number of documents where it occurs
        Output: (term, document=n/N)
    Reducer:
        D = total number of documents in corpus. This can be passed by the driver as a constant;
        d = number of documents in corpus where the term appears. It is a counter over the reduced values for each term;
        TFIDF = n/N * log(D/d);
        Output: ((word in document), TFIDF)*/
	    	 
	    	Job job = Job.getInstance(new Configuration(), "Job #3");
	        
	    	 job.setJarByClass(TFIDF.class);
	        job.setMapperClass(WordsTFIDFMapper.class);
	        job.setReducerClass(WordsTFIDFReducer.class);
	 
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	 
	        FileInputFormat.addInputPath(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));
	 
	        Path inputPath = new Path("input/");
	        FileSystem fs = inputPath.getFileSystem(job.getConfiguration());
	        FileStatus[] stat = fs.listStatus(inputPath);
	        job.setJobName(String.valueOf(stat.length));
	        
	        return job.waitForCompletion(true);
	    }
	    
	    // Job for sorting results
	    public boolean job4(String in, String out) throws Exception {

			Job job = Job.getInstance(new Configuration(), "Job #4");
			
			job.setJarByClass(TFIDF.class);
			
			FileInputFormat.setInputPaths(job, new Path(in));
			FileOutputFormat.setOutputPath(job, new Path(out));
			
			job.setMapperClass(SortMapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			return job.waitForCompletion(true);
			
		}
	    

}
 