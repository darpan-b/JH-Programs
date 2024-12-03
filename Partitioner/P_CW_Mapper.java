import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class P_CW_Mapper extends Mapper<Object, Text, Text, Text> {

    private Text gender = new Text();
    private Text asalary = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by commas (assuming CSV format)
        String[] fields = value.toString().split(",");

        // Extract the fields: age, gender, salary
        String ageStr = fields[2].trim();
        String genderStr = fields[3].trim();
        String salaryStr = fields[4].trim()+fields[5].trim();

        try {
            //~ int age = Integer.parseInt(ageStr);
            //~ int salaryValue = Integer.parseInt(salaryStr);
            
            //~ System.out.println("SALARY VALUE = " + salaryValue);

            // Set the gender and salary
            gender.set(genderStr);
            // value is composite: (age,salary) 
            String as = ageStr + "," + salaryStr;
            System.out.println("as = " + as);
            asalary.set(as);

            // Emit the gender as the key and the salary as the value
            context.write(gender, asalary);
        } catch (NumberFormatException e) {
            // Skip any malformed lines
        }
    }
}
