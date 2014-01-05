import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
	ArrayList<Double>updateCentroids(ArrayList<String> centroids){
		
		ArrayList<Double> newDimensions = new ArrayList<Double>();
		for(int i =0; i< centroids.get(0).length()-1;i++){
			newDimensions.add(0.0);
		}
		for(int i = 0;i<centroids.size();i++){
			String tempCentroids= centroids.get(i);
			String [] temp = tempCentroids.split(",");
			String id = temp[0];
			for(int i =1 ; i<temp.length;i++){
				
			}
		}
	}
	protected void reduce(Text arg0, Iterable<Text> arg1, Context context)  throws IOException, InterruptedException{
		Map<Integer, ArrayList<String>> centroids = new HashMap<Integer,ArrayList<String>>();
		for(Text str : arg1){
			if(centroids.containsKey(Integer.parseInt(str.toString()))){
				ArrayList<String> temp = centroids.get(str);
				temp.add(arg0.toString());
				centroids.put(Integer.parseInt(str.toString()), temp);
			}
			else{
				ArrayList<String> temp = new ArrayList<String>();
				temp.add(arg0.toString());
				centroids.put(Integer.parseInt(str.toString()), temp);
			}
		}
		
	}
	/**
	 * @param args
	 */
	

}
