import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapClass extends Mapper<LongWritable,Text,Text,Text>{
	 HashMap<Integer,ArrayList<Double>> centers = null;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public void setup(Context context) throws IOException {
		Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
		String line = null;
		
		 centers = new HashMap<Integer,ArrayList<Double>>();
		while((line = br.readLine())!=null){
			String [] temp = line.split(",");
			ArrayList<Double> axis = new ArrayList<Double>();
			for(int i = 2; i<temp.length; i++){
				axis.add(Double.parseDouble(temp[i]));
			}
			centers.put(Integer.parseInt(temp[0]), axis);
		}
	}
	
	public static int CalculateDis(String [] dimensions, HashMap<Integer,ArrayList<Double>> centers){
		double minDistance = Double.MAX_VALUE;
		int minCenter = 1;
		ArrayList<Double> instance = new ArrayList<Double>();
		for(Integer i : centers.keySet()){
			double distance = 0.0;
			for(int j =2; j<dimensions.length;j++){
				distance += Math.pow(Double.parseDouble(dimensions[j]) - centers.get(i).get(j-2),2);
			}
			if(distance<minDistance){
				minDistance = distance;
				minCenter = i;
				
			}
		}	
		return minCenter;
	}
	
	// 'value' is one instance. The first string is the case id which should be ignored. 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
		    String [] dimensions = value.toString().split(",");
			int minCenter = CalculateDis (dimensions, centers); 
			StringBuilder str = new StringBuilder();
			for( int i = 1; i<dimensions.length;i++){
				str.append(dimensions[i]);
				str.append(',');
			}
			context.write(new Text(Integer.toString(minCenter)),new Text(str.toString()));
	}

}
