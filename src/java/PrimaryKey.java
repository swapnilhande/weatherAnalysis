package java;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import edu.neu.mapreduce.project.PrimaryKey;

public class PrimaryKey implements WritableComparable<PrimaryKey> 
{
	IntWritable  stnid ;
	IntWritable  year ;
	
	public PrimaryKey(IntWritable stnid, IntWritable year) {
		this.stnid = stnid;
		this.year = year;
	}
	
	PrimaryKey() {
		this.stnid = new IntWritable();
		this.year = new IntWritable();
	}
	
	public IntWritable getStnId() {
		return stnid;
	}
	

	public IntWritable getYear() {
		return year;
	}
	
	public void setStnId(String stnId){
		this.stnid = new IntWritable(Integer.parseInt(stnId));
	}
	
	public void setYear(String year){
		this.year = new IntWritable(Integer.parseInt(year));
	}

	
	  @Override
	public int compareTo(PrimaryKey arg0) {
		
		PrimaryKey k = arg0;
		int cmp = 0;
		cmp = this.stnid.compareTo(k.stnid);
		if (cmp != 0) {
			return cmp;
		} else
			return this.year.compareTo(k.year);
	}

	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stnid == null) ? 0 : stnid.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		PrimaryKey other = (PrimaryKey) obj;
		if (stnid == null) {
			if (other.stnid != null)
				return false;
		} else if (!stnid.equals(other.stnid))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	    stnid.readFields(in);
	    year.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	    
	    stnid.write(out);
	    year.write(out);
		
	}
	
	

}