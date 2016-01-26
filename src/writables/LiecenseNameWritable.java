package writables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;


public class LiecenseNameWritable extends LiecenseWritable  {

	private String _name;
	private String _id;
	public String get_name() {
		return _name;
	}

	public void set_name(String _name) {
		this._name = _name;
	}
	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		_name = WritableUtils.readString(dataInput);
		_id = WritableUtils.readString(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(dataOutput, _name);
		WritableUtils.writeString(dataOutput, _id);
	}
	@Override 
	public String toString(){
		return _name +","+_id;
	}

	

}
