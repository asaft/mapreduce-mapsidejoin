package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import writables.LicenseNameWritable;
import writables.LicenseTypeWritable;



public class MapSideMapper extends Mapper<Text, TupleWritable , Text, Text> 
{
	Text _keyOut = new Text(); 
	Text _outValue = new Text(); 
	@Override 
	public void map(Text key, TupleWritable value,    Context context) throws IOException ,InterruptedException 
	{
		LicenseNameWritable name = (LicenseNameWritable) value.get(0);
		LicenseTypeWritable type = (LicenseTypeWritable) value.get(1);
		_outValue.set(type.get_liecenseType());
		_keyOut.set(name.get_name());
		context.write(_keyOut,_outValue);

		
		
	}
}
	
 
