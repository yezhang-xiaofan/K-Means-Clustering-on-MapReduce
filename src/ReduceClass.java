import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<Text,Text,Text,Text>{

	/**
	 * @param args
	 */
	public enum Counter{
		CONVERGED
	}
	public void reduce(Text key, <Iterable>Text values, Context context) throws IOException, InterruptedException{
		context.getCounter(Counter.CONVERGED).increment(1);
	}
	
}
