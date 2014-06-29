package kmeans;

import org.apache.commons.lang.StringUtils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AggregatorStation {

	public static class AggregatorStationMapper extends
	Mapper<Object, Text, Text, TempAttribute> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			try{
				// Get Record Line
				String[] inputRecord = value.toString().replaceAll("\\t", " ").
						replaceAll("\\s+"," ").split(" ");
				// Initialize station Id , date and temp
				String stnId;
				String date;
				String temp;
				if(inputRecord.length >= 3 && StringUtils.isNumeric(inputRecord[0]) && 
						StringUtils.isNumeric(inputRecord[2]) &&
						inputRecord[2].length() >= 6){

					// assign stationId, date, temp
					stnId = inputRecord[0];
					date = inputRecord[2];
					temp = inputRecord[3];

					//extract year and month from date
					String year = date.substring(0,4);
					String month = date.substring(4, 6);
					
					// Create Temperature Attribute as Value Pair
					TempAttribute tempAtr = new TempAttribute();
					tempAtr.setMonth(new IntWritable(Integer.parseInt(month)));
					tempAtr.setTemp(new FloatWritable(Float.parseFloat(temp)));		
					context.write(new Text(stnId), tempAtr);
				}
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}


	public static class AggregatorStationReducer extends
	Reducer<Text, TempAttribute, Text, Text> {

		public void reduce(Text key, Iterable<TempAttribute> values, Context context)
				throws IOException, InterruptedException {
			try{
			StringBuilder sb = new StringBuilder();
			// Compute average monthly sum for each station ID
			float tempmontlyAvg[] = new float[13];
			int counttemp[] = new int[13];
			for (TempAttribute value : values){
				int month = value.getMonth().get();
				if (value.getTemp().get() != 9999.9f) {
					counttemp[month]++;
					tempmontlyAvg[month] += value.getTemp().get();
				}
			}
			for (int i = 1; i < 13; i++) {
				sb.append(tempmontlyAvg[i] / Math.max(1, counttemp[i]) + " ");
			}
			// Output station Id with monthly average values. 
			context.write(new Text(key + ","), new Text(
					sb.toString()));
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
			System.err.println("Usage: Temp data <in> <out>");
			System.exit(2);
		}
		
		String inpPath = otherArgs[0];
		String outPath = otherArgs[1];
		Job job = new Job(conf, "Temp Aggregator ");
		job.setJarByClass(AggregatorStation.class);
		job.setMapperClass(AggregatorStationMapper.class);
		job.setReducerClass(AggregatorStationReducer.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TempAttribute.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inpPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
