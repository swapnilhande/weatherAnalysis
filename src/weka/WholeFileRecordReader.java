package weka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import weka.core.Instances;

class WholeFileRecordReader extends RecordReader<Text, Instances>
{

    private FileSplit fileSplit;
    private Configuration conf;

    private Instances value;
    private boolean processed = false;

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
        System.out.println("initialize in whole record reader");
    }
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed)
        {   
            System.out.println("next key value");
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            //FileInputStream in = null;
            try
            {
            	ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream("instances.txt"));
    			Instances wekaInstance = (Instances) ois.readObject();
    			/*ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    			ObjectOutputStream objectOutputStream = new ObjectOutputStream(
    					arrayOutputStream);
    			objectOutputStream.writeObject(wekaInstance);
                IOUtils.readFully(in, arrayOutputStream.toByteArray(), 0, contents.length);*/
                value = wekaInstance;
               
            } catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    
            finally
            {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(this.fileSplit.getPath().getName());
    }
   
    @Override
    public Instances getCurrentValue() throws IOException,
    InterruptedException
    {
        return new Instances(value);
    }
    @Override
    public float getProgress() throws IOException
    {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        //     do nothing
    }
   
}
