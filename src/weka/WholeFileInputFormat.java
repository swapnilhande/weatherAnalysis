package weka;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import weka.core.Instances;

public class WholeFileInputFormat extends FileInputFormat<Text, Instances>{
   
    @Override
    protected boolean isSplitable(JobContext context, Path file){
        System.out.println("set isplitable");
        return false;
       
    }
   
    @Override
    public  RecordReader<Text, Instances> createRecordReader
    (InputSplit split,TaskAttemptContext context) throws IOException,
    InterruptedException {
               WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(split,context);
        return reader;
       
    }
}