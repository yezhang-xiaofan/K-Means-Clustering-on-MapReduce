import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class CombineClass extends Reducer<Text, Text, Text, Text>{
	
	 /*
	public void setup(Context context) throws IOException{
		Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
		String line;
		Map<Integer,ArrayList<String>> oldcentroids = new HashMap<Integer,ArrayList<String>>();
		while((line=br.readLine())!=null){
			String [] temp = line.split(",");		
		}
	}
	*/
	
	//arg0 is the cluster id, arg1 is data instance
	protected void reduce(Text arg0, Iterable<Text>arg1, Context context)  throws IOException, InterruptedException{
		//centroids stores the 'cluster id, number of instances' pair
		Map<Integer, ArrayList<String>> centroids = new HashMap<Integer,ArrayList<String>>();
		ArrayList<ArrayList<Double>> instances = new ArrayList<ArrayList<Double>>();
		for(Text str : arg1){
            if(!str.toString().equals("null")){
				String [] instance = str.toString().split(",");
				ArrayList <Double> temp = new ArrayList<Double>();
				for (int i = 0; i<instance.length;i++){
					temp.add(Double.parseDouble(instance[i]));
				}
				instances.add(temp);
            }
		}
			
		//calculate the average value for the cluster
		int num_Instance = instances.size();
		ArrayList<Double> average = new ArrayList<Double>();
		if(instances.size()!=0){
			for (int j = 0;j<instances.get(0).size();j++){
				average.add(0.0);
			}
		}
		for (int j = 0; j<instances.size();j++){
			for (int k = 0;k<instances.get(0).size();k++){
				double temp = average.get(k);
				temp += instances.get(j).get(k);
				 average.set(k, temp);
 			}
		}
		for (int j = 0; j<average.size();j++){
			average.set(j, average.get(j)/num_Instance);
		}
		
		StringBuilder str = new StringBuilder();
		
		//str is the average feature value for the cluster  + number of instances 
		for (int j = 0;j<average.size();j++){
			str.append(average.get(j).toString());
			str.append(",");
		}
		if(num_Instance ==0){
			context.write(arg0, new Text("null"));
		}
		else{
			str.append(num_Instance);
			context.write(arg0, new Text(str.toString()));		
		}
	}
}
			 
	/**
	 * @param args
	 */
	


