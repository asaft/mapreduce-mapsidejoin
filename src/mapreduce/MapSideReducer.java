package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MapSideReducer extends Reducer<Text, Text, Text, Text> 
{
private Text _valueOut = new Text();
 
public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
	Iterator<Text> it = values.iterator();
	String licenses ="";
	int count = 1; 
	while (it.hasNext()){
		 
		licenses+=""+count+"."+it.next().toString();
		 
		count++;
	}
	
	_valueOut.set(licenses);
	context.write(key, _valueOut);
	
	

}
}
