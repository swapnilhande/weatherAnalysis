package weka;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class WekaPredictor {

    public static class WekaPredictorMapper extends
            Mapper<Object, Text, Text, NullWritable> {
        private List<Classifier> classifierList;

        private static Instance fillInstance(String stnId, String month,
                String day, String fog, String rain, String snow,
                String hail,
                String thunder, String tornado, FastVector wekaAttributes) {
            Instance instance = new Instance(9);
            instance.setValue((Attribute) wekaAttributes.elementAt(0),
                    Integer.parseInt(stnId));
            instance.setValue((Attribute) wekaAttributes.elementAt(1),
                    Integer.parseInt(month));
            instance.setValue((Attribute) wekaAttributes.elementAt(2),
                    Integer.parseInt(day));
            instance.setValue((Attribute) wekaAttributes.elementAt(3), fog);
            instance.setMissing(0);
            instance.setValue((Attribute) wekaAttributes.elementAt(5), snow);
            instance.setValue((Attribute) wekaAttributes.elementAt(6), hail);
            instance.setValue((Attribute) wekaAttributes.elementAt(7),
                    thunder);
            instance.setValue((Attribute) wekaAttributes.elementAt(8),
                    tornado);
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

        public void setup(Context context) throws IOException,
                InterruptedException {
            classifierList = new ArrayList<>();
            List<Classifier> classifierList = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                FileSystem fileSystem = FileSystem
                        .get(new Configuration());
                Path path = new Path("/classifier" + i + ".list");
                // For Aws: Read from HDFS
                FSDataInputStream in = fileSystem.open(path);
                ObjectInputStream ois = new ObjectInputStream(in);
                Classifier classifier;
                try {
                    classifier = (Classifier) ois.readObject();
                    classifierList.add(classifier);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                System.out.println("Classifier List Size: "
                        + classifierList.size());
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] inputRecord = value.toString().replaceAll("\\t", " ")
                    .replaceAll("\\s+", " ").split(" ");
            String stnId = inputRecord[0];
            if (StringUtils.isNumeric(stnId)) {
                FastVector wekaAttributes = getWekaAttributes();
                Instances testSet = new Instances("rel", wekaAttributes, 10);
                testSet.setClassIndex(7);
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
                Instance instance = fillInstance(key.toString(), month, day,
                        fog, rain, snow, hail, thunder, tornado,
                        wekaAttributes);
                if (instance != null) {
                    testSet.add(instance);
                }
                testSet.setClassIndex(4);
                try {
                    int yes = 0;
                    int no = 0;
                    for (Classifier wekaClassifier : classifierList) {
                        double prediction = wekaClassifier
                                .classifyInstance(testSet
                                        .firstInstance());
                        if (prediction == 1.0) {
                            yes++;
                        }
                        if (prediction == 0) {
                            no++;
                        }
                    }
                    if (yes > no) {
                        testSet.firstInstance().setClassValue("yes");
                    } else {
                        testSet.firstInstance().setClassValue("no");
                    }
                    context.write(
                            new Text(stnId + "," + month + "," + day + ","
                                    + fog + ","
                                    + testSet.firstInstance().stringValue(4)
                                    + "," + snow + "," + hail + "," + thunder
                                    + "," + tornado), NullWritable.get());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];
        Job job = new Job(conf, "Weka");
        job.setJarByClass(WekaPredictor.class);
        job.setMapperClass(WekaPredictorMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
