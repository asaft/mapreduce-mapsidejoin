package main;

import java.io.IOException;

import mapreduce.MapSideMapper;
import mapreduce.MapSideReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import writables.LiecenseNameWritable;
import writables.LiecenseTypeWritable;


public class Driver {
	static Configuration _configuration;
	private static String NAMES="NAMES";
	private static String TYPES="TYPES";
	public static void main(String[] args) throws IOException {
		
		_configuration = new Configuration();
		Job job = Job.getInstance(_configuration);   
		// TODO Auto-generated method stub
		_configuration.set(NAMES, LiecenseNameWritable.class.getName());
		_configuration.set(TYPES, LiecenseTypeWritable.class.getName());
		
		String joinExpression  = CompositeInputFormat.compose("inner", 
				LiecenseInputFormat.class,new Path(args[0]),new Path(args[1]));
		System.out.println(joinExpression);
		_configuration.set("mapreduce.join.expr", joinExpression);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapSideMapper.class);
		job.setReducerClass(MapSideReducer.class);
		job.setInputFormatClass(CompositeInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setJarByClass(Driver.class);   
	}

}
