package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeanFinalAlgorithm {

	public static class KMeanMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				// Split Input File and get single record
				String[] input = value.toString().replaceAll("\\t", "")
						.replaceAll("\\s+", " ").split(",");
				if (input.length < 1) {
					throw new Exception();
				}
				
				// get the average monthly temperature values 
				String[] tempData = input[1].split(" ");
				double[] dist = new double[5];
				for (int i = 0; i < 5; i++) {
					// get the cluster centroid values from conf
					String[] k = conf.get("k" + Integer.toString(i + 1)).split(
							" ");
					// compute distance between input data and cluster centroids
					dist[i] = computeDistance(tempData, k);
				}

				// Assign minimum computed distance cluster to the input data as key
				// output temp values.
				String clusterKey = "k" + minValue(dist);
				context.write(new Text(clusterKey), new Text(input[1]));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static int minValue(double[] distance) {
		int key = 0;
		double min = distance[0];
		for (int i = 1; i < distance.length; i++) {
			if (distance[i] < min) {
				min = distance[i];
				key = i;
			}
		}
		return key + 1;
	}

	private static double computeDistance(String[] data, String[] clusterPoint) {
		double sum = 0.0;
		int count = 0 ;
		
		// compute distance between the cluster centroid points and input data
		for (int i = 0; i < 12; i++) {
			if(Double.parseDouble(data[i])==0.0){
				count++;
			}
			sum += Math
					.pow((Double.parseDouble(data[i]) - Double
							.parseDouble(clusterPoint[i])), 2);

		}
		if(count > 4){
			return Double.MAX_VALUE;
		}else
			return sum;
	}

	public static class KMeanReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double[] tempMnthlySum = new double[12];

			double[] newKpoint = new double[12];
			int count = 0;
			// For all cluster input points
			for (Text val : values) {
				count++;
				String[] tempData = val.toString().split(" ");
				
				// calculate sum of monthly temperature of all points in that cluster
				for (int i = 0; i < tempData.length - 1; i++) {
					tempMnthlySum[i] += Double.parseDouble(tempData[i]);
				}
			}



			//Compute the monthly sum of cluster points
			for (int i = 0; i < tempMnthlySum.length; i++) {
				newKpoint[i] = tempMnthlySum[i] / count;
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < newKpoint.length; i++) {

				if (i == newKpoint.length - 1) {
					sb.append(newKpoint[i]);
				}
				else{
					sb.append(newKpoint[i] + " ");
				}
			}
			// output cluster key and new centroid value
			context.write(new Text(key + ":"), new Text(sb.toString()));

		}
	}

	// This mapper is called once the new cluster points are created
	// Map only job to print out cluster centroid values locally.
	public static class KMeanFinalMapper extends
	Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				String[] data = value.toString().replaceAll("\\t", "")
						.replaceAll(" +", " ").split(",");

				String[] tempData = data[1].split(" ");

				double[] dist = new double[5];
				for (int i = 0; i < 5; i++) {
					String[] k = conf.get("k" + Integer.toString(i + 1)).split(
							" ");
					dist[i] = computeDistance(tempData, k);
				}

				String clusterKey = "k" + minValue(dist);
				
				// compute cluster key 
				if(dist[0]!=Double.MAX_VALUE){
					context.write(new Text(clusterKey), new Text(data[0]));
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Kmean <in> <out>");
			System.exit(2);
		}
		String inpPath = otherArgs[0];
		String outPutPath = otherArgs[1];
		String finalOutputPath = otherArgs[2];
		// Set initial cluster centroid values in conf as key value pairs
		conf.set(
				"k1",
				"23.52903 22.417858 27.57333 35.081818 41.696774 47.456665 61.758064 55.806446 47.306667 37.445156 34.41 27.277418");
		conf.set(
				"k2",
				"-22.448385 -28.607143 -21.338709 0.62999994 15.5034485 36.666668 45.425797 0.0 36.12759 28.9129 10.303333 -12.174193");
		conf.set(
				"k3",
				"28.283869 19.617857 13.022578 24.640001 35.87419 43.503334 44.651615 43.6871 39.406662 29.127777 0.0 0.0");
		conf.set(
				"k4",
				"51.40556 45.674995 54.509674 64.10999 70.593544 79.483345 83.09677 82.035484 73.08334 65.03226 57.09667 46.0");
		conf.set(
				"k5",
				"-32.9129 -40.425797 -29.509674 -23.696774 -11.12777 -9.696774 -7.03345 -8.12759 -12.52903 -17.38709 -21.50344 -28.25797");
		
		int count = 0;
		HashMap<String, String> rerun = new HashMap<String, String>();

		while (true) {
			Job job = new Job(conf, " Kmean ");
			job.setJarByClass(KMeanFinalAlgorithm.class);
			job.setMapperClass(KMeanMapper.class);
			job.setReducerClass(KMeanReducer.class);
			job.setNumReduceTasks(4);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inpPath));
			FileOutputFormat.setOutputPath(job, new Path(outPutPath + count));
			if (job.waitForCompletion(true)) {
				// Check if further iteration is needed.
				if (checkforFurtherIterations(outPutPath + count, conf, rerun)) {
					count++;
					// update conf centroid values.
					updateConf(conf, rerun);
				// else break
				} else {
					break;
				}

			}
		}
		System.out.println("No Of iterations:" + count);
		// Final Job
		Job job = new Job(conf, " Kmean ");
		job.setJarByClass(KMeanFinalAlgorithm.class);
		job.setMapperClass(KMeanFinalMapper.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inpPath));
		FileOutputFormat.setOutputPath(job, new Path(finalOutputPath));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}

	private static void updateConf(Configuration conf,
			HashMap<String, String> rerun) {

		for (String key : rerun.keySet()) {
			System.out.println(rerun.get(key));
			conf.set(key, rerun.get(key));

		}

	}

	private static boolean checkforFurtherIterations(String outputPath, Configuration conf,
			HashMap<String, String> rerun) {
		try {
			// process for unwanted files.
			FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
			FileStatus[] status = fs.listStatus(new Path(outputPath));
			for (int i = 0; i < status.length; i++) {
				// get rid of _SUCCESS file
				if (status[i].getPath().getName().toLowerCase()
						.contains("_success")) {
					continue;
				}
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(status[i].getPath())));
				String first = null;
				try {
					first = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				while (first != null) {
					// do not parse .crc files
					if (!first.toLowerCase().startsWith("crc")) {
						String[] data = first.replaceAll("\\t", "").split(
								"[:]");
						if (data[0] != null) {
							rerun.put(data[0], data[1]);
						}
					}
					try {
						first = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			System.out.println("File not found");
			e.printStackTrace();
		}

		boolean isFurtherIterationRequired = false;
		for (String key : rerun.keySet()) {
			// Calculate previous and new cluster values and get difference 
			String[] oldValue = conf.get(key).split(" ");
			String[] newValue = rerun.get(key).split(" ");
			for (int i = 0; i < 12; i++) {
				if (Math.abs(Double.parseDouble(oldValue[i])
						- Double.parseDouble(newValue[i])) > 0.01) {
					isFurtherIterationRequired = true;
					break;
				}
			}
		}
		return isFurtherIterationRequired;
	}
}
