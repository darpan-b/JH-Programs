import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EndSemMapper extends Mapper<Object, Text, Text, Text> {

    private Text itemT = new Text();
    private Text arT = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by commas (assuming CSV format)
        String[] fields = value.toString().split(",");
		if(fields.length != 5) {
			return;
		}
		System.out.println("fields length = " + fields.length);
        // Extract the fields: age, gender, salary
        String item = fields[2].trim();
        String amountStr = fields[3].trim();
        String region = fields[4].trim();

        try {
            //~ int age = Integer.parseInt(ageStr);
            //~ int salaryValue = Integer.parseInt(salaryStr);
            
            //~ System.out.println("SALARY VALUE = " + salaryValue);

            // Set the gender and salary
            //~ gender.set(genderStr);
            // value is composite: (age,salary) 
            //~ String as = ageStr + "," + salaryStr;
            //~ System.out.println("as = " + as);
            //~ asalary.set(as);

			itemT.set(item);
			String amountAndRegion = amountStr + "," + region;
			arT.set(amountAndRegion);


            // Emit the gender as the key and the salary as the value
            context.write(itemT, arT);
            
            
        } catch (Exception e) {
            // Skip any malformed lines
        }
    }
}
