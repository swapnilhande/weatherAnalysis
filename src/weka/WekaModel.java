package weka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class WekaModel {

	public static class WekaModelMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] inputRecord = value.toString().replaceAll("\\t", " ")
					.replaceAll("\\s+", " ").split(" ");

			String stnId = inputRecord[0];
			if (StringUtils.isNumeric(stnId)) {
				// get the date
				String date = inputRecord[2];
				String month = date.substring(4, 6);
				String day = date.substring(6);
				String fog = inputRecord[21].charAt(0) == '1' ? "yes" : "no";
				String rain = inputRecord[21].charAt(1) == '1' ? "yes" : "no";
				String snow = inputRecord[21].charAt(2) == '1' ? "yes" : "no";
				String hail = inputRecord[21].charAt(3) == '1' ? "yes" : "no";
				String thunder = inputRecord[21].charAt(4) == '1' ? "yes"
						: "no";
				String tornado = inputRecord[21].charAt(5) == '1' ? "yes"
						: "no";
				for (int i = 0; i < 5; i++) {
					double probability = Math.random();
					if (probability < 0.1) {
						context.write(new IntWritable(i), new Text(stnId + "-"
								+ month + "-" + day + "-" + fog + "-" + rain
								+ "-" + snow + "-" + hail + "-" + thunder + "-"
								+ tornado));
					}
				}
			}
		}
	}

	public static class WekaModelPartitioner extends
			Partitioner<IntWritable, Text> {

		@Override
		public int getPartition(IntWritable key, Text value, int noOfPartitions) {
			return key.get() % noOfPartitions;

		}
	}

	public static class WekaModelReducer extends
			Reducer<IntWritable, Text, BytesWritable, NullWritable> {

		private static Instance fillInstance(String stnId, String month,
				String day, String fog, String rain, String snow, String hail,
				String thunder, String tornado, FastVector wekaAttributes) {
			Instance instance = new Instance(9);
			instance.setValue((Attribute) wekaAttributes.elementAt(0),
					Integer.parseInt(stnId));
			instance.setValue((Attribute) wekaAttributes.elementAt(1),
					Integer.parseInt(month));
			instance.setValue((Attribute) wekaAttributes.elementAt(2),
					Integer.parseInt(day));
			instance.setValue((Attribute) wekaAttributes.elementAt(3), fog);
			instance.setValue((Attribute) wekaAttributes.elementAt(4), rain);
			instance.setValue((Attribute) wekaAttributes.elementAt(5), snow);
			instance.setValue((Attribute) wekaAttributes.elementAt(6), hail);
			instance.setValue((Attribute) wekaAttributes.elementAt(7), thunder);
			instance.setValue((Attribute) wekaAttributes.elementAt(8), tornado);
			return instance;
		}

		private static FastVector getWekaAttributes() {
			Attribute stationId = new Attribute("stationId");
			Attribute month = new Attribute("month");
			Attribute day = new Attribute("day");

			FastVector isFog = new FastVector(2);
			isFog.addElement("yes");
			isFog.addElement("no");
			Attribute fog = new Attribute("fog", isFog);

			FastVector isRain = new FastVector(2);
			isRain.addElement("yes");
			isRain.addElement("no");
			Attribute rain = new Attribute("rain", isRain);

			FastVector isSnow = new FastVector(2);
			isSnow.addElement("yes");
			isSnow.addElement("no");
			Attribute snow = new Attribute("snow", isSnow);

			FastVector isHail = new FastVector(2);
			isHail.addElement("yes");
			isHail.addElement("no");
			Attribute hail = new Attribute("hail", isHail);

			FastVector isThunder = new FastVector(2);
			isThunder.addElement("yes");
			isThunder.addElement("no");
			Attribute thunder = new Attribute("thunder", isThunder);

			FastVector isTornado = new FastVector(2);
			isTornado.addElement("yes");
			isTornado.addElement("no");
			Attribute tornado = new Attribute("tornado", isTornado);

			FastVector wekaAttributes = new FastVector(8);
			wekaAttributes.addElement(stationId);
			wekaAttributes.addElement(month);
			wekaAttributes.addElement(day);
			wekaAttributes.addElement(fog);
			wekaAttributes.addElement(rain);
			wekaAttributes.addElement(snow);
			wekaAttributes.addElement(hail);
			wekaAttributes.addElement(thunder);
			wekaAttributes.addElement(tornado);
			return wekaAttributes;
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) {

			FastVector wekaAttributes = getWekaAttributes();
			Instances trainingSet = new Instances("rel", wekaAttributes, 10);
			trainingSet.setClassIndex(7);
			for (Text value : values) {
				String[] data = value.toString().split("-");
				String stnId = data[0];
				String month = data[1];
				String day = data[2];
				String fog = data[3];
				String rain = data[4];
				String snow = data[5];
				String hail = data[6];
				String thunder = data[7];
				String tornado = data[8];
				Instance instance = fillInstance(stnId, month, day, fog, rain,
						snow, hail, thunder, tornado, wekaAttributes);
				if (instance != null) {
					trainingSet.add(instance);
				}
			}
			// Column which we want to predict
			trainingSet.setClassIndex(4);
			Classifier wekaClassifier = new J48();
			try {
				wekaClassifier.buildClassifier(trainingSet);
				// For AWS: Write to HDFS
				ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(
						arrayOutputStream);
				objectOutputStream.writeObject(wekaClassifier);
				FileSystem fileSystem = FileSystem.get(new Configuration());
				Path path = new Path("/classifier" + key.get() + ".list");
				FSDataOutputStream out = fileSystem.create(path);
				out.write(arrayOutputStream.toByteArray());
				System.out.println("File: /classifier" + key.get() + ".list");
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String outputPath = args[1];
		Job job = new Job(conf, "Weka");
		job.setJarByClass(WekaModel.class);
		job.setMapperClass(WekaModelMapper.class);
		job.setReducerClass(WekaModelReducer.class);
		job.setNumReduceTasks(5);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(WekaModelPartitioner.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
