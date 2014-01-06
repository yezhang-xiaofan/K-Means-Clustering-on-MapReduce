
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
				
		int iteration = 1;
		long changes = 0;
		Path dataPath = new Path(args[0]);

		//read in the initial cluster centroids.
		Configuration centroidConf = new Configuration();
        Job centroidInputJob = new Job(centroidConf);
        centroidInputJob.setJobName("KMeans Centroid Input");
        centroidInputJob.setJarByClass(Main.class);

        Path centroidsPath = new Path("centroids_0");
        
        centroidInputJob.setMapperClass(KmeansCentroidInputMapper.class);
        
        // No Combiner, no Reducer.
        
        centroidInputJob.setMapOutputKeyClass(Text.class);
        centroidInputJob.setMapOutputValueClass(Text.class);
        centroidInputJob.setOutputKeyClass(Text.class);
        centroidInputJob.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(centroidInputJob,new Path(args[1]) );
        FileOutputFormat.setOutputPath(centroidInputJob, centroidsPath);
        centroidInputJob.setNumReduceTasks(0);
        
        if (!centroidInputJob.waitForCompletion(true)) {
            System.err.println("Centroid input job failed!");
            System.exit(1);
        }
        
		while(true){
			Configuration conf = new Configuration();
			Path nextIter = new Path(String.format("centroids_%s", iteration));
			Path prevIter = new Path(String.format("centroids_%s", iteration - 1));
			conf.set(Main.CENTROIDS, prevIter.toString());
			
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
