package main;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LicenseOutputFormat extends TextOutputFormat {

	public static final String OUTPUT_FILE_NAME = "OutputFileName";
	public static final String LICENSE = "License";
	public static final String NAMES = "Names"; 
	protected String _fileName; 
	
	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		// TODO Auto-generated method stub
		super.checkOutputSpecs(job);
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		// TODO Auto-generated method stub
		FileOutputCommitter committer = 
			      (FileOutputCommitter) getOutputCommitter(context);
		_fileName = context.getConfiguration().get(OUTPUT_FILE_NAME);
			    return new Path(committer.getWorkPath(), _fileName);
	}

	@Override
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub
		return super.getOutputCommitter(arg0);
	}

}
