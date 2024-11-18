import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CityTemperatureJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CityTemperatureJob <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Temperature");

        // Set the main class and the jar file containing the job
        job.setJarByClass(CityTemperatureJob.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(CityTemperatureMapper.class);
        job.setReducerClass(CityTemperatureReducer.class);

        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);  // City name
        job.setOutputValueClass(DoubleWritable.class);  // Temperature

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and wait for it to complete
        if (job.waitForCompletion(true)) {
            // After the job completes, check the custom counter value
            long invalidEntries = job.getCounters().findCounter(InvalidEntriesCounter.INVALID_RECORDS).getValue();
            System.out.println("Number of invalid records: " + invalidEntries);
            System.exit(0);
        } else {
            System.exit(1);
        }

        // Run the job and wait for it to complete
        //~ System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
