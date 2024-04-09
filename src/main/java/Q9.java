import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q9 {
    public static class MapperClass extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("samereducer"), value);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            double previousHotness = 0;
            String[] previous = null;
            String builder = "";

            for (Text t: value) {
                double currentHotness = Double.parseDouble(t.toString().split("\t")[0]);
                String[] curr = t.toString().split("\t")[1].split("\\|");

                for (int i = 1; i < curr.length - 1; i++) {
                    if (previous == null) break;
                    try {
                        double change = Double.parseDouble(curr[i]) - Double.parseDouble(previous[i]);
                        builder += change + "|";
                    } catch (NumberFormatException nfe) {continue;}
                }
                
                context.write(new Text(Double.toString(currentHotness - previousHotness)), new Text(builder));
                previous = curr;
                previousHotness = currentHotness;
                builder = "";
            }
        }
    }

    public static class MeanReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            double[] arr = new double[11];
            for (Text t: value) {
                String[] stats = t.toString().split("\\|");
                for (int i = 0; i < stats.length; i++) {
                    arr[i] = Double.parseDouble(stats[i]);
                }
            }

            String finalOutput = Arrays.stream(arr).mapToObj(x -> Double.toString(x)).collect(Collectors.joining(", "));
            context.write(new Text("CHANGES: "), new Text(finalOutput));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set("mapred.textoutputformat.separator", "|");
        Job job = Job.getInstance(config, "q9 changes");
        job.setJarByClass(Q9.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q9 change average");
            job2.setJarByClass(Q9.class);
            job2.setMapperClass(MapperClass.class);
            job2.setReducerClass(MeanReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
