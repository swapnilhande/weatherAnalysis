package java;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import edu.neu.mapreduce.project.PrimaryKey;
import edu.neu.mapreduce.project.TempKey;

public class TempKey implements WritableComparable<TempKey> 
{
	PrimaryKey key;
	DoubleWritable  temp ;

	public TempKey(PrimaryKey key, DoubleWritable temp) {		
		this.key = key;
		this.temp = temp;
	}

	TempKey() {
		this.key = new PrimaryKey();
		this.temp = new DoubleWritable();
	}

	public PrimaryKey getPrimaryKey() {
		return key;
	}
	
	public DoubleWritable getTemp() {
		return temp;
	}
	
	public void setPrimaryKey(PrimaryKey key){
		this.key = key;
	}
	
	public void setTemp(String temp){
		this.temp = new DoubleWritable(Double.parseDouble(temp));
	}

	@Override
	public int compareTo(TempKey arg0) {

		TempKey k = arg0;
		int cmp = 0;
		cmp = this.key.compareTo(k.key);
		if (cmp != 0) {
			return cmp;
		} else{
				return this.temp.compareTo(k.temp);
		}
			
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((temp == null) ? 0 : temp.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TempKey other = (TempKey) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (temp == null) {
			if (other.temp != null)
				return false;
		} else if (!temp.equals(other.temp))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		key.readFields(in);
		temp.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		key.write(out);
		temp.write(out);

	}

}
