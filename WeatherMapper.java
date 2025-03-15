import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text year = new Text();
    private IntWritable temperature = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 0) {
            try {
                String yearString = line.substring(15, 19);  // Extracting Year (Positions 15-19)
                int temp = Integer.parseInt(line.substring(87, 92).trim());  // Extracting Temperature (Positions 87-92)
                year.set(yearString);
                temperature.set(temp);
                context.write(year, temperature);
            } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
                // Skip lines with incorrect formatting
            }
        }
    }
}
