import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EndSemDriver {

    public static void main(String[] args) throws Exception {
        // Set up the configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales per region");

        // Set the job's jar class
        job.setJarByClass(EndSemDriver.class);

        // Set the Mapper, Reducer, and Partitioner
        job.setMapperClass(EndSemMapper.class);
        job.setReducerClass(EndSemReducer.class);
        job.setPartitionerClass(EndSemPartitioner.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set custom OutputFormat
        //~ job.setOutputFormatClass(CustomOutputFormat.class);

        // Set the number of reducers (one for each partition/age range)
        job.setNumReduceTasks(4); // One for each partition

        // Set the output path for custom RecordWriter
        //~ conf.set("output.path", args[1]);

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
