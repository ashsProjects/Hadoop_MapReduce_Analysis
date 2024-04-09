import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleSorter {
    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String word = tokens[0];
            double count = Double.parseDouble(tokens[1]);
            context.write(new DoubleWritable(count), new Text(word));
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
