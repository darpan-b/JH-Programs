import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultipleInputReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    // Variable to hold the common separator
    private static String commonSeparator;

    // Setup method to retrieve the common separator for the output file
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        
        // Retrieving the common file separator from context for output file
        commonSeparator = configuration.get("Separator.Common");
    }

    // Reduce function to process the input key-value pairs
    @Override
    public void reduce(LongWritable key, Iterable<Text> textValues, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        String[] firstFileValues = null, secondFileValues = null;
        String[] stringValues;
        int fval = 0;

        // Process each Text value
        for (Text textValue : textValues) {
            // Split the string based on the common separator
            stringValues = textValue.toString().split(commonSeparator);
            
            // Check if the record belongs to the first file (F1)
            if ("F1".equalsIgnoreCase(stringValues[0])) {
                firstFileValues = Arrays.copyOf(stringValues, stringValues.length);
                fval++; // Increment fval if it's from the first file
            }
            
            // Check if the record belongs to the second file (F2)
            if ("F2".equalsIgnoreCase(stringValues[0])) {
                secondFileValues = Arrays.copyOf(stringValues, stringValues.length);
            }
        }

        // If there is at least one record from the first file (F1)
        if (fval > 0) {
            // Append the values from the first file (excluding the first value which is the file tag)
            for (int index = 1; index < firstFileValues.length; index++) {
                stringBuilder.append(firstFileValues[index] + commonSeparator);
            }
        } else {
            return; // If no records from F1, skip this record
        }

        // If there is a record from the second file (F2), append it to the result
        if (secondFileValues != null) {
            for (int index = 1; index < secondFileValues.length; index++) {
                stringBuilder.append(secondFileValues[index] + commonSeparator);
            }
        }

        // Debugging output to print the reducer's result
        System.out.println("Reducer Output: " + stringBuilder.toString());
        System.out.println("Number of F1 Records: " + fval);

        // Write the final key-value pair to the output
        context.write(key, new Text(stringBuilder.toString()));
    }
}
