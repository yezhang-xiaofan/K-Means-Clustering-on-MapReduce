import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<Text,Text,Text,Text>{

	/**
	 * @param args
	 */
	//values is 'intermediate cluster centroied + number of instances'
	
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int total_Instance = 0;
			for (Text str : values){
				String [] temp = values.toString().split(",");
				ArrayList<Double> average = new ArrayList<Double>();
				for (int i= 0;i<temp.length-1;i++){
					average.add(Double.parseDouble(temp[i]));
				}
				int num_Instance = Integer.parseInt(temp[temp.length-1]);
				total_Instance += num_Instance;
				for (int i = 0;i<average.size();i++){
					average.set(i, average.get(i)*num_Instance);
				}
			}
			
			context.write(key, value);
		}
	
	
}
