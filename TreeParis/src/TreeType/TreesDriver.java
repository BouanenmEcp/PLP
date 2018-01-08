package TreeType;
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

public class TreesDriver {
	
	public static String IN_PATH = "Input/arbres.csv";
    public static String OUT_PATH = "outputTreeType";
	
	public static void main(String[] args) throws Exception {
		

	    FileSystem fs = FileSystem.get(new Configuration());  
        if (fs.exists(new Path(TreesDriver.OUT_PATH)))
            fs.delete(new Path(TreesDriver.OUT_PATH), true);
	    
        //Defnir une nouveau job
		JobClient client = new JobClient();
		JobConf conf = new JobConf(TreesDriver.class);
		conf.setJobName("TreesType");
		
		// Format Data Output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Definir Mapper Reducer
		conf.setMapperClass(TreesMapper.class);
		conf.setReducerClass(TreesReducer.class);

		// Specifier Formats Data Input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Specifier repertoires input and output
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