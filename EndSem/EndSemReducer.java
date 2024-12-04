
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EndSemReducer extends Reducer<Text, Text, Text, Text> {

    private Text maxSalary = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Find the maximum salary for each gender
        int maxval = Integer.MIN_VALUE;
        
        int totSales = 0;

		for(Text val: values) {
			String[] ar = val.toString().split(",");
			int amount = Integer.parseInt(ar[0]);
			totSales += amount;
		}

		context.write(key, new Text(Integer.toString(totSales)));


        //~ for (Text val : values) {
			//~ // Split the value into age and salary
            //~ String[] ageSalary = val.toString().split(",");
            
            //~ // Parse the salary from the second part of the value (after the comma)
            //~ int curSalary = Integer.parseInt(ageSalary[1]);
            //~ System.out.println("cursalary = " + curSalary);

            //~ // Update the maximum salary if the current one is greater
            //~ maxval = Math.max(maxval, curSalary);
			//~ int curSalary = Integer.parseInt(val.toString().split(",")[1]);
            //~ max = Math.max(max, curSalary);
        //~ }

		//~ String mvs = Integer.toString(maxval);
        //~ // Set the maximum salary and write the result
        //~ maxSalary.set(mvs); // IntWritable diye keno emit kora jacche janina, shokal theke chesta korchi
							//~ // tried everything I could, finally had to settle with emitting Text
        //~ context.write(key, maxSalary);
    }
}
