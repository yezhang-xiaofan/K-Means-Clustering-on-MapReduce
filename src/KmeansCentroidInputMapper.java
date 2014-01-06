import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KmeansCentroidInputMapper extends Mapper<LongWritable,Text,Text,Text>{

	/**
	 * @param args
	 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException,IOException{
			String [] elements = value.toString().trim().split(",");
			StringBuilder str = new StringBuilder();
		    Integer clusterId  = Integer.parseInt(elements[0]); 
		    for (int i = 2;i<elements.length;i++){
		    		str.append(elements[i]+",");
		    }
		    context.write(new Text(clusterId.toString()), new Text(str.toString()));
		}
	}
	
