import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CityTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable minTemperature = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double minTemp = Double.MAX_VALUE;

        // Iterate through all the temperature readings for a particular city and find the minimum
        for (DoubleWritable val : values) {
            minTemp = Math.min(minTemp, val.get());
        }

        // Set the minimum temperature and write the city name along with the min temperature
        minTemperature.set(minTemp);
        context.write(key, minTemperature);
    }
}
