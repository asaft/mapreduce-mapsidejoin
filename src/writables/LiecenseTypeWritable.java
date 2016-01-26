package writables;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;


public class LiecenseTypeWritable extends LiecenseWritable {

	private String _liecenseType;
	private String _id;
	
	


	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		_liecenseType = WritableUtils.readString(dataInput);
		_id = WritableUtils.readString(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(dataOutput, _liecenseType);
		WritableUtils.writeString(dataOutput, _id);
	}

	@Override 
	public String toString(){
		return _liecenseType+","+_id;
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public String get_liecenseType() {
		return _liecenseType;
	}

	public void set_liecenseType(String _liecenseTypes) {
		this._liecenseType = _liecenseTypes;
	}

}
