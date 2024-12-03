import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CustomRecordWriter extends RecordWriter<Text, IntWritable> {

    private FSDataOutputStream maleOut;
    private FSDataOutputStream femaleOut;

    public CustomRecordWriter(FileSystem fs, Path malePath, Path femalePath) throws IOException {
        // Open the output streams for male and female files
        maleOut = fs.create(malePath);
        femaleOut = fs.create(femalePath);
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        // Write to the appropriate file based on the key (male or female)
        if (key.toString().equalsIgnoreCase("Male")) {
            maleOut.writeBytes(key.toString() + "\t" + value.get() + "\n");
        } else if (key.toString().equalsIgnoreCase("Female")) {
            femaleOut.writeBytes(key.toString() + "\t" + value.get() + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // Close both output streams when done
        if (maleOut != null) {
            maleOut.close();
        }
        if (femaleOut != null) {
            femaleOut.close();
        }
    }
}



