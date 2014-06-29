package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TempAttribute implements WritableComparable<WeatherAttributes>{
	// Initialize month and temp values.
	IntWritable month ;
	FloatWritable temp;

	public TempAttribute() {
		this.month = new IntWritable() ;
	    this.temp = new FloatWritable();
	}
	public TempAttribute(IntWritable month, FloatWritable temp) {
		super();
		this.month = month;
		this.temp = temp;
	}
	public IntWritable getMonth() {
		return month;
	}
	public void setMonth(IntWritable month) {
		this.month = month;
	}
	public FloatWritable getTemp() {
		return temp;
	}
	public void setTemp(FloatWritable temp) {
		this.temp = temp;
	}
	

	@Override
	public int compareTo(WeatherAttributes w) {
		return (this.month.compareTo(w.month));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		month.write(out);
		temp.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		month.readFields(in);
		temp.readFields(in);
	}

}


