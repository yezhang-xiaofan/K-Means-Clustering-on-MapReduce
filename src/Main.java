
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.*;


public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		int iteration = 0;
		//counter from the previous running import job
		
	
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		long counter = job.getCounters().findCounter(ReduceClass.Counter.CONVERGED).getValue();
		
		iteration ++;
		while(counter > 0){
			 conf = new Configuration();
			conf.set("recursion.iter",iteration+" ");
			 job = new Job(conf);
			
			job.setJobName("KMeans "+ iteration);
			job.setMapperClass(MapClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			job.setCombinerClass(CombineClass.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			Path oldCentersFile = new Path("files/kmeans/iter_" + (iteration-1) + "/");
			Path newCentersFile = new Path("files/kmeans/iter_" + iteration+"/");
			DistributedCache.addCacheFile(oldCentersFile.toUri(),conf);
			FileSystem fs1 = FileSystem.get(oldCentersFile.toUri(), conf);
			FileSystem fs2 = FileSystem.get(newCentersFile.toUri(),conf);
			FileInputFormat.addInputPath(job, new Path("files/kmeans/iter_" + (iteration-1) + "/"));
			FileOutputFormat.setOutputPath(job, new Path("files/kmeans/iter_" + iteration));
					
			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters().findCounter(ReduceClass.Counter.CONVERGED).getValue();
			
		}
	}

}
