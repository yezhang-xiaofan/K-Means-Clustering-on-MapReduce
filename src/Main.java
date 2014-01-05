
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.*;


public class Main{

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	static enum Counter{
		CONVERGED
	}
	
	public static final String CENTROIDS = "centroids";
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		int iteration = 1;
		long changes = 0;
		Path dataPath = new Path("data");

		while(true){
			Configuration conf = new Configuration();
			conf.setInt("number of clusters", 8);
			conf.setFloat("tolerance", 1e-6F);
			
			Path nextIter = new Path(String.format("centrods_%s", iteration));
			Path prevIter = new Path(String.format("centroids_%s", iteration - 1));
			Job job = new Job(conf);
		    job.setJobName("Kmeans " + iteration);
			job.setJarByClass(Main.class);
			
			job.setJobName("KMeans "+ iteration);
			
			//Set Mapper, Combiner, and Reducer
			job.setMapperClass(MapClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			job.setCombinerClass(CombineClass.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			//Set input/output paths
			FileInputFormat.addInputPath(job, dataPath);
			FileOutputFormat.setOutputPath(job, nextIter);
			
			job.setNumReduceTasks(1);
			job.waitForCompletion(true);
			iteration++;
			changes = job.getCounters().findCounter(Main.Counter.CONVERGED).getValue();
			job.getCounters().findCounter(Main.Counter.CONVERGED).setValue(0);
			if(changes<=0){
				break;
			}		
		}
	}

}
