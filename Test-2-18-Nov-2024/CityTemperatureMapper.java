import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

public class CityTemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text cityName = new Text();
    private DoubleWritable temperature = new DoubleWritable();

    // Define a counter variable
    private Counter invalidEntriesCounter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize the custom counter
        invalidEntriesCounter = context.getCounter(InvalidEntriesCounter.INVALID_RECORDS);
    }



    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Sample input format: New York, 2024-11-18 00:00, 15.2
        // DATA FORMAT: Date,city,morning temp,afternoon temp,evening temp
        String[] parts = value.toString().split(",");
        if (parts.length != 5) {
            // If the record is invalid, increment the custom counter
            invalidEntriesCounter.increment(1);
            return;  // Skip this record
        }
        else {
			cityName.set(parts[1].trim());  // City name
			try {
				temperature.set(Math.min(Math.min(Double.parseDouble(parts[2].trim()),
										 Double.parseDouble(parts[3].trim())),
										 Double.parseDouble(parts[4].trim())));  // Temperature (converted to integer)
			context.write(cityName, temperature);

			} catch(Exception e) {
				
				invalidEntriesCounter.increment(1);
				return;
			}
		
			// Emit city name as key, and temperature as value
			//~ context.write(cityName, temperature);
        }
    }
}
