package weka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

public class WekaTestData {

    public static class WekaTestDataMapper extends
            Mapper<Object, Text, Text, NullWritable> {
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

                context.write(new Text(stnId + "," + month + "," + day + ","
                        + fog + "," + rain + "," + snow + "," + hail + ","
                        + thunder + "," + tornado), NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];
        Job job = new Job(conf, "Weka");
        job.setJarByClass(WekaTestData.class);
        job.setMapperClass(WekaTestDataMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
