import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomOutputFormat extends FileOutputFormat<Text, IntWritable> {

    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // Get the output directory from the job configuration
        Path outputDir = FileOutputFormat.getOutputPath(job);
        FileSystem fs = outputDir.getFileSystem(job.getConfiguration());

        // Define the file paths for male and female output in the same directory
        Path maleFile = new Path(outputDir, "male_output.txt");
        Path femaleFile = new Path(outputDir, "female_output.txt");

        // Return a custom RecordWriter
        return new CustomRecordWriter(fs, maleFile, femaleFile);
    }
}
