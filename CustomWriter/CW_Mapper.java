import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CW_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text gender = new Text();
    private IntWritable salary = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by commas (assuming CSV format)
        String[] fields = value.toString().split(",");

        // Extract the fields: age, gender, salary
        String ageStr = fields[2].trim();
        String genderStr = fields[3].trim();
        String salaryStr = fields[4].trim()+fields[5].trim();

        try {
            // Set the gender and salary
            gender.set(genderStr);
            // value is composite: (age,salary) 
            salary.set(Integer.parseInt(salaryStr));

            // Emit the gender as the key and the salary as the value
            context.write(gender, salary);
        } catch (NumberFormatException e) {
            // Skip any malformed lines
        }
    }
}
