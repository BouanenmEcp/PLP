package hadoop.pagerank;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class PageRank {
    
    public static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 2;
    public static String IN_PATH = "input/soc-Epinions1.txt";
    public static String OUT_PATH = "output";
    
    public static void main(String[] args) throws Exception {
    	  
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(PageRank.OUT_PATH)))
            fs.delete(new Path(PageRank.OUT_PATH), true);
        
        String inPath = null;;
        String lastOutPath = null;
        PageRank pagerank = new PageRank();
        
        System.out.println("Running Job#1 (graph parsing) ...");
        boolean isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/step00");
        if (!isCompleted) {
            System.exit(1);
        }
        
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUT_PATH + "/step" + NF.format(runs);
            lastOutPath = OUT_PATH + "/step" + NF.format(runs + 1);
            System.out.println("Running Job#2 [" + (runs + 1) + "/" + PageRank.ITERATIONS + "] (PageRank calculation) ...");
            isCompleted = pagerank.job2(inPath, lastOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        
        System.out.println("Running Job#3 (rank ordering) ...");
        isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("DONE!");
        System.exit(0);
    }
    
    
     //Job for Graph Parsing which parses the graph given as input and initializes the page rank.
    public boolean job1(String in, String out) throws Exception {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(GraphParsingMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(GraphParsingReducer.class);
        
        return job.waitForCompletion(true);
     
    }
    
    // job for Rank Calculation which calculates the new ranking and generates the same output format as the input
    public boolean job2(String in, String out) throws Exception {
        
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(RankCalculationMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(RankCalculationReducer.class);

        return job.waitForCompletion(true);
        
    }
    
    // job for sorting
    public boolean job3(String in, String out) throws Exception {
        
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(PageRank.class);
        
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(SortMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
        
    }
    
}