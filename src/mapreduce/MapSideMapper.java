package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;



public class MapSideMapper extends Mapper<Text, TupleWritable , Text, IntWritable> 
{
	Text _keyOut = new Text(); 
	IntWritable _outValue = new IntWritable(); 
	@Override 
	public void map(Text key, TupleWritable value,    Context context) throws IOException ,InterruptedException 
	{
	//	StudentNameWritable name = (StudentNameWritable) value.get(0);
	//	StudentGradeWritable grade = (StudentGradeWritable) value.get(1);
	//	_outValue.set(grade.get_grade());
	//	_keyOut.set(name.get_name());
	//	context.write(_keyOut,_outValue);

		
		
	}
}
	
 
