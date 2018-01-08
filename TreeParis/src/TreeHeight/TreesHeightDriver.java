package TreeHeight;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import TreeType.TreesDriver;

public class TreesHeightDriver {
	
	public static String IN_PATH = "Input/arbres.csv";
    public static String OUT_PATH = "outputTreeHeight";
	
	public static void main(String[] args) throws Exception {
		
		// Supprimer le repertoire ouput s'il existe
	    FileSystem fs = FileSystem.get(new Configuration());  
        if (fs.exists(new Path(TreesDriver.OUT_PATH)))
            fs.delete(new Path(TreesDriver.OUT_PATH), true);
        
		JobClient client = new JobClient();

		// Creer un nouveau job
		JobConf conf = new JobConf(TreesHeightDriver.class);
		conf.setJobName("TreesHeight");
		
		// type Output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Definir Mapper Rducer
		conf.setMapperClass(TreesHeightMapper.class);
		conf.setReducerClass(TreesHeightReducer.class);

		// Definir Formats input et output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Specifier les repertoires input ouput
		FileInputFormat.setInputPaths(conf, new Path(IN_PATH));
		FileOutputFormat.setOutputPath(conf, new Path(OUT_PATH));
		 
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}