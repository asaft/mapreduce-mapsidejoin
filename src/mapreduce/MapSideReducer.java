package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MapSideReducer extends Reducer<Text, IntWritable, Text, FloatWritable> 
{
private FloatWritable _valueOut = new FloatWritable();
 
public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
	Iterator<IntWritable> it = values.iterator();
	int sum = 0;
	int count = 0; 
	while (it.hasNext()){
		 
		sum+=it.next().get();
		count++;
	}
	float average = sum /count;
	_valueOut.set(average);
	context.write(key, _valueOut);
	
	

}
}
