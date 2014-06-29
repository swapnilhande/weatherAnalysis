package java;

//import required jars
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// class to find maximum temperature at different stations across different years
public class MaxTemp {

	/*
	 * Custom Mapper
	 * The mapper takes in the weather input file as input
	 * and emits the station Id, year and temperature
	 * We have used Secondary sort to sort the value of
	 * temp by keeping it in the key
	 */
	public static class MaxTempMapper extends
	Mapper<Object, Text, TempKey, Text>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try{
				
				String[] inputRecord = value.toString().replaceAll("\\t", " ")
						.replaceAll("\\s+", " ").replaceAll("\\*", "").split(" ");

				// get the Station Id
				String stnId;
				if(StringUtils.isNumeric(stnId = inputRecord[0])){

					// get the max temperature field
					String maxTemp = inputRecord[17];

					// get the date field
					String day = inputRecord[2];

					//extract year from date
					String year = day.substring(0,4);

					//set Primary Key
					PrimaryKey naturalKey = new PrimaryKey();
					naturalKey.setStnId(stnId);
					naturalKey.setYear(year);
					
					//set Secondary Key
					TempKey secondaryKey = new TempKey();
					secondaryKey.setPrimaryKey(naturalKey);
					secondaryKey.setTemp(maxTemp);

					// emit Station Id, year and temp in Secondary key
					// and date in value
					context.write(secondaryKey, new Text(day));
				}
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}

	/*
	 * Custom Partitioner
	 * partitions based in station id and year
	 */
	public static class MaxTempPartitioner extends Partitioner<TempKey, Text> {

		@Override
		public int getPartition(TempKey key, Text value, int numPartitions) {
			return (key.getPrimaryKey().hashCode()) % numPartitions;
		}
	}

	/*
	 * Custom Key Comparator
	 * sort based on station and year in ascending order and
	 * temperature in descending order
	 */
	public static class MaxTempKeyComparator extends WritableComparator {

		protected MaxTempKeyComparator() {
			super(TempKey.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			TempKey k1 = (TempKey) w1;
			TempKey k2 = (TempKey) w2;
			int cmp = 0;
			cmp = k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
			if (cmp != 0) {
				return cmp;
			}
			else
				return -k1.getTemp().compareTo(k2.getTemp());
		}
	}

	/*
	 * Custom Grouping Comparator
	 * groups based on Station Id and Year
	 */
	public static class MaxTempGroupComparator extends WritableComparator {

		protected MaxTempGroupComparator() {
			super(TempKey.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			TempKey k1 = (TempKey) w1;
			TempKey k2 = (TempKey) w2;	
			return k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
		}
	}


	/*
	 * Custom Reducer
	 * emits the data for max temperature 
	 * which is the first record in the list of values.
	 * Ignored 9999.9 values which signifies 
	 * wrong data
	 */
	public static class MaxTempReducer extends
	Reducer<TempKey, Text, Text, Text> {

		protected void reduce(TempKey key, Iterable<Text> values,
				Context context) throws java.io.IOException,
				InterruptedException {
			try{

			for (Text day : values) {
				DoubleWritable maxTemp = key.getTemp();
				if(maxTemp.get() == 9999.9)
					continue;
				IntWritable stnId = key.getPrimaryKey().getStnId();
				IntWritable year = key.getPrimaryKey().getYear();

				context.write(new Text( stnId + "," + year + "," + maxTemp),
						new Text ("," + day));
				break;
			}
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: maximum temperature <in> <out>");
			System.exit(2);
		}
		String inpPath = otherArgs[0];
		String outPutPath = otherArgs[1];

		// job to find Maximum Temperature
		Job job = new Job(conf, "MaxTemp");
		
		//set required job parameters
		job.setJarByClass(MaxTemp.class);
		job.setMapperClass(MaxTempMapper.class);
		job.setPartitionerClass(MaxTempPartitioner.class);
		job.setSortComparatorClass(MaxTempKeyComparator.class);
		job.setGroupingComparatorClass(MaxTempGroupComparator.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setMapOutputKeyClass(TempKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Set number of reduce tasks to 10
		job.setNumReduceTasks(10);
		
		FileInputFormat.addInputPath(job, new Path(inpPath));
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

