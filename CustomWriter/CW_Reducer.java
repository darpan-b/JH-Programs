
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CW_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable maxSalary = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Find the maximum salary for each gender
        int maxval = Integer.MIN_VALUE;

        for (IntWritable val : values) {

            // Update the maximum salary if the current one is greater
            maxval = Math.max(maxval, val.get());
        }

		maxSalary.set(maxval);
        context.write(key, maxSalary);
    }
}
