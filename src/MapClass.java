import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
	 private int num_Clusters;
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(Main.CENTROIDS));
		FileSystem fs = FileSystem.get(centroidsPath.toUri(),conf);
		FileStatus [] list = fs.globStatus(new Path(centroidsPath,"part-*"));
		centers = new HashMap<Integer,ArrayList<Double>>();
		for (int j = 0;j<list.length;j++){
			BufferedReader br = new BufferedReader(new 
					InputStreamReader(fs.open(list[j].getPath())));
			String line;
			while((line = br.readLine())!=null){
				String [] temp = line.split("[,\\t]");
				ArrayList<Double> axis = new ArrayList<Double>();
				for(int i = 1; i<temp.length; i++){
					axis.add(Double.parseDouble(temp[i]));
				}
				centers.put(Integer.parseInt(temp[0]), axis);
			}
		}
		num_Clusters = centers.size();
	}
	
	public static int CalculateDis(String [] dimensions, HashMap<Integer,ArrayList<Double>> centers){
		double minDistance = Double.MAX_VALUE;
		int minCenter = 1;
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
		    if(!Pattern.matches("[a-zA-Z]+", dimensions[0])){
				int minCenter = CalculateDis (dimensions, centers); 
				StringBuilder str = new StringBuilder();
				for( int i = 1; i<dimensions.length;i++){
					str.append(dimensions[i]);
					str.append(',');
				}
				for (int i = 1;i<=num_Clusters;i++){
					if(i==minCenter){
						context.write(new Text(Integer.toString(minCenter)),new Text(str.toString()));
					}
					else{
						context.write(new Text(new Integer(i).toString()), new Text("null"));
					}
				}			
		    }
	}
}
