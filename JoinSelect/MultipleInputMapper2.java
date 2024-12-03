import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleInputMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {

    // Static variables for separators and file tag
    private static String separator;
    private static String commonSeparator;
    private static final String FILE_TAG = "F2"; // Tag for file 2

    // Setup method to retrieve configuration settings for separators
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        
        // Retrieving the file separator for file2 from the configuration
        separator = configuration.get("Separator.File2");

        // Retrieving the common separator for writing data to the reducer
        commonSeparator = configuration.get("Separator.Common");
    }

    // Map function to process each input row
    @Override
    public void map(LongWritable rowKey, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line based on the separator
        String[] values = value.toString().split(separator);
        StringBuilder stringBuilder = new StringBuilder();

        // Build a string with the values (excluding the first value) separated by the common separator
        for (int index = 1; index < values.length; index++) {
            stringBuilder.append(values[index] + commonSeparator);
        }

        // Check if the first column (values[0]) is not null or "NULL"
        if (values[0] != null && !"NULL".equalsIgnoreCase(values[0])) {
            // Write the output to the context with the key as the first column (converted to Long)
            // and the value as the concatenated string with the file tag and other columns
            context.write(new LongWritable(Long.parseLong(values[0])), 
                          new Text(FILE_TAG + commonSeparator + stringBuilder.toString()));
        }
    }
}
