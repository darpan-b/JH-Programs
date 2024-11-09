//~ 3. Take inputData.csv as input. Consider amount as price*quantity and price as unit price of the product.
//~ A. Develop a Hadoop Application to generate total amount for each product for all transactions.
//~ Output needs to be sorted alphabetically according to product name
//~ B. Log total time taken by Mapper in a file in local file system.
//~ Hint: You may use System.nanoTime().

// THIS PROGRAM SUMS UP THE QUANTITIES OF THE PRODUCTS



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.log4j.Logger;



public class GenTotAmount {
	
	public static class GenTotAmountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Logger logger_m = Logger.getLogger(GenTotAmountMapper.class);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] row = line.split(",");	
			if (row[0].equals("Date")) {
				return;
			}
			System.out.println("In mapper: Processing key: " + row[1] + ", value: " + Integer.parseInt(row[3]));
			//~ logger_m.info("In mapper: Processing key: " + row[1] + ", value: " + Integer.parseInt(row[3]));
			context.write(new Text(row[1]), new IntWritable(Integer.parseInt(row[3])));
			System.out.println("Mapper called, line = " + line);
		}
	}
	
	public static class GenTotAmountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private static final Logger logger_r = Logger.getLogger(GenTotAmountReducer.class);
			
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			System.out.println("In reducer: Processing key: " + key.toString() + ", value: " + sum);
			//~ logger_r.info("In reducer: Processing key: " + key.toString() + ", value: " + sum);

			//~ System.out.println("sum = " + sum);
			//~ context.write(new Text("Total number of products sold are:"), new IntWritable(0));
			context.write(key, result);
		}	
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");  // Set local mode
        Job job = Job.getInstance(conf, "get total amount");
        job.setJarByClass(GenTotAmount.class);
        job.setMapperClass(GenTotAmountMapper.class);
        //~ job.setCombinerClass(GenTotAmountReducer.class);
        job.setReducerClass(GenTotAmountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
}
