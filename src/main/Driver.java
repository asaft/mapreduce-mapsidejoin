package main;

import java.io.IOException;

import mapreduce.MapSideMapper;
import mapreduce.MapSideReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MapSideJoin.LicenseInputFormat;
import writables.LicenseNameWritable;
import writables.LicenseTypeWritable;



public class Driver extends Configured implements Tool{
	Configuration _configuration;
	private ControlledJob  setSortingJob(String input, String output,String outputFileName)
			throws Exception{
		 
		_configuration.set(LicenseOutputFormat.OUTPUT_FILE_NAME, outputFileName);
		ControlledJob jc = new ControlledJob(_configuration);
		
		Job job = jc.getJob();          
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		
		job.setOutputFormatClass(LicenseOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setJarByClass(Driver.class);
		return jc; 
	}
	private ControlledJob setMRJob(String input1,String input2,String output) throws IOException{
		 
		_configuration.set(LicenseOutputFormat.NAMES, LicenseNameWritable.class.getName());
		_configuration.set(LicenseOutputFormat.LICENSE, LicenseTypeWritable.class.getName());
		
		String joinExpression  = CompositeInputFormat.compose("inner", 
				LicenseInputFormat.class,new Path(input1),new Path(input2));
		System.out.println(joinExpression);
		_configuration.set("mapreduce.join.expr", joinExpression);
		ControlledJob controlledJob = new ControlledJob(_configuration );
		Job job = controlledJob.getJob();
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapSideMapper.class);
		job.setReducerClass(MapSideReducer.class);
		job.setInputFormatClass(CompositeInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setJarByClass(Driver.class);      

		return controlledJob; 
	}
	public static void main(String[] args) throws Exception {  
		int result = ToolRunner.run(new Configuration(), new Driver(),args);
		System.exit(result);
		
		                                          
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 5) {           
			System.out.println("usage: [input1] [output1] [input2] [output2] [finaloutput]");           
			System.exit(-1);         
		}
	
		_configuration = this.getConf();
		System.out.println(_configuration.get("Hello"));
		ControlledJob nameJob = setSortingJob(args[0],args[1],LicenseOutputFormat.NAMES);
		ControlledJob licenseJob= setSortingJob(args[2],args[3],LicenseOutputFormat.LICENSE);
		ControlledJob mrJob= setMRJob(args[1], args[3], args[4]);
		mrJob.addDependingJob(nameJob);
		mrJob.addDependingJob(licenseJob);
		JobControl jobControl = new JobControl("MyJob");
		jobControl.addJob(nameJob);
		jobControl.addJob(licenseJob);
		jobControl.addJob(mrJob);
		
		//jobControl.run();
		Thread thread = new Thread(jobControl);
		thread.start();
		
		while (!jobControl.allFinished()){
			System.out.println("Running");
			Thread.sleep(5000);
		}
		System.out.println("<<<Done>>>");
		return 0; 
	} 
}