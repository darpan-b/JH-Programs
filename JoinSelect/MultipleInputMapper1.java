import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleInputMapper1 extends Mapper<LongWritable, Text, LongWritable, Text>
{
	private static String separator;
	private static String commonSeparator;
	private static String FILE_TAG="F1";
	public void setup(Context context)
	{
		Configuration configuration = context.getConfiguration();
		//Retrieving the file separator from context for file1.
		separator = configuration.get("Separator.File1");

		//Retrieving the file separator from context for writing the data to reducer.
		commonSeparator=configuration.get("Separator.Common");
	}
	@Override
	public void map(LongWritable rowKey, Text value,
	Context context) throws IOException, InterruptedException
	{
		String[] values = value.toString().split(separator);
		StringBuilder stringBuilder = new StringBuilder();
		double val = Double.parseDouble(values[3]);
		if (val <= 20.2)
			return;
		for(int index=1;index<values.length;index++)
		{
				stringBuilder.append(values[index]+commonSeparator);
		}
		if(values[0] != null && !"NULL".equalsIgnoreCase(values[0]))
		{
			context.write(new LongWritable(Long.parseLong(values[0])), new Text(FILE_TAG+commonSeparator+stringBuilder.toString()));
		}
	}
}
