import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class AgeRangePartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // Assume the key is "gender" and the value is "salary"
        String[] ga = value.toString().split(",");
        int age = Integer.parseInt(ga[0]);

        // Implement partitioning logic based on age ranges
        int partition = 0;

        //~ if (gender.equals("Male") || gender.equals("Female")) {
            // Logic for partitioning based on age ranges (just an example)
            if (age >= 18 && age <= 30) {
                partition = 0;  // Age range 18-30
            } else if (age >= 31 && age <= 40) {
                partition = 1;  // Age range 31-40
            } else if (age >= 41 && age <= 50) {
                partition = 2;  // Age range 41-50
            } else {
                partition = 3;  // Other ages
            }
        //~ }

        return partition;
    }
}
