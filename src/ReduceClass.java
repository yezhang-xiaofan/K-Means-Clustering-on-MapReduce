import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ReduceClass extends Reducer<Text,Text,Text,Text>{

	/**
	 * @param args
	 */
	 HashMap<Integer,ArrayList<Double>> centers = new HashMap<Integer,ArrayList<Double>>();
	public void setup(Context context) throws IOException {
		
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(Main.CENTROIDS));
		BufferedReader br = new BufferedReader(new FileReader(centroidsPath.toString()));
		String line = null;
		while((line = br.readLine())!=null){
			String [] temp = line.split(",");
			ArrayList<Double> axis = new ArrayList<Double>();
			for(int i = 2; i<temp.length; i++){
				axis.add(Double.parseDouble(temp[i]));
			}
			centers.put(Integer.parseInt(temp[0]), axis);
		}
		br.close();
	}
	//key is 'cluster id'
	//values is 'intermediate cluster centroid + number of instances'
	
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double total_Instance = 0.0;
			ArrayList<Double> sum_Instances = new ArrayList<Double>();
			for (Text str : values){
				String [] temp = values.toString().split(",");
				ArrayList<Double> average = new ArrayList<Double>();
				for (int i= 0;i<temp.length-1;i++){
					average.add(Double.parseDouble(temp[i]));
				}
				int num_Instance = Integer.parseInt(temp[temp.length-1]);
				total_Instance += num_Instance;
				for (int i = 0;i<average.size();i++){
					sum_Instances.set(i, sum_Instances.get(i)+average.get(i)*num_Instance);
				}
			}		
			
			for (int j = 0;j<sum_Instances.size();j++){
				sum_Instances.set(j,sum_Instances.get(j)/total_Instance);
			}
			
			ArrayList<Double> previous = centers.get(Integer.parseInt(key.toString()));
		    
			double difference  = 0.0;
			for (int i = 0; i<sum_Instances.size(); i++){
				difference += Math.pow((sum_Instances.get(i) - previous.get(i)),2);
			}
			difference = Math.sqrt(difference); 
			if(difference > 0.000001F){
				context.getCounter(Main.Counter.CONVERGED).increment(1);
			}
			StringBuilder str1 = new StringBuilder();
			for (int j = 0; j<sum_Instances.size();j++){
				str1.append(sum_Instances.get(j).toString()+",");
			}
			context.write(key, new Text(str1.toString()));
		}
	
	
}
