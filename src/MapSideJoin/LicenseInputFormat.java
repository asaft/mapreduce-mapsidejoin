package MapSideJoin;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import writables.LicenseNameWritable;
import writables.LicenseTypeWritable;
import writables.LicenseWritable;



public class LicenseInputFormat extends FileInputFormat<Text,LicenseWritable> {

	@Override
	public RecordReader<Text, LicenseWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new LicenseRecordReader();
	}
	
	

	private class LicenseRecordReader extends RecordReader<Text, LicenseWritable>{
		Configuration _configuration; 
		private Path _file = null;
		private FSDataInputStream  _in; 
		Text _currentKey = new Text(); 
		LicenseWritable _currentValue ; 
		long _start,_end,_pos; 
		BufferedReader _reader = null;
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			_in.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return _currentKey;
		}

		@Override
		public LicenseWritable getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return _currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (_start == _end){
				return 0.0f;
			}
			
			return Math.min(1.0f, (_pos - _start) / (float)(_end - _start));
		}

		private LicenseWritable CreateLicenseWritable() throws ClassNotFoundException, InstantiationException, IllegalAccessException{
			String writableClass = _configuration.get(_file.getName());
			Class cls = Class.forName(writableClass);
			return (LicenseWritable)cls.newInstance();
					
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			FileSplit filesplit = (FileSplit) split;
	        _file = filesplit.getPath();
	        _configuration= context.getConfiguration();
	        FileSystem fs = _file.getFileSystem(_configuration);
	        
	        _in = fs.open(_file);
	        _start = filesplit.getStart();
	        _end = _start+filesplit.getLength();
	        _pos = _start;
	        _reader = new BufferedReader(new InputStreamReader(_in));
	        System.out.println("Input Format Initialize:"+_file.getName());
	        try {
				_currentValue = CreateLicenseWritable();
			} catch (ClassNotFoundException | InstantiationException
					| IllegalAccessException e) {
				// TODO Auto-generated catch block
				System.out.println("Cant Create Class");
				_currentValue = null; 
			}
	      
	        	
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String line = _reader.readLine();
			
			
			boolean retVal = false; 
			if (line != null){
				retVal = true;
				//Here we will split the line and get the key value pairs
				String []kv = line.split(",");
				_currentKey.set(kv[0]);
				if (_currentValue instanceof  LicenseNameWritable){
					LicenseNameWritable lnw = (LicenseNameWritable) _currentValue;
					lnw.set_id(kv[0]);
					lnw.set_name(kv[1]);
				}
				else{
					LicenseTypeWritable type = (LicenseTypeWritable) _currentValue;
					System.out.println(kv[0]);
					type.set_id(kv[0]);
					type.set_liecenseType(kv[1]);
						
				}
				_pos = _in.getPos();
				System.out.println("File pos"+_pos);
			
			}
			
			return retVal;
		}
		
	}

	

}
