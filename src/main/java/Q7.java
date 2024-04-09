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

public class Q7 {
    public static class StartMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[17];
            val = val.substring(1, val.length() -1);
            context.write(new Text("Segments Start"), new Text(val));
        }
    }

    public static class PitchMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[19];
            val = val.substring(1, val.length() -1);
            context.write(new Text("Segments Pitches"), new Text(val));
        }
    }

    public static class TimbreMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[20];
            val = val.substring(2, val.length() -1);
            context.write(new Text("Segments Timbre"), new Text(val));
        }
    }

    public static class LMaxMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[21];
            val = val.substring(1, val.length() -1);
            context.write(new Text("Segments Loudness Max"), new Text(val));
        }
    }

    public static class LMaxTimeMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[22];
            val = val.substring(1, val.length() -1);
            context.write(new Text("Segments Loudness Max Time"), new Text(val));
        }
    }

    public static class LStartMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().split("\\|")[23];
            val = val.substring(1, val.length() -1);
            context.write(new Text("Segments Loudness Start"), new Text(val));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int numValues = value.iterator().next().toString().split(" ").length;
            double[] sums = new double[numValues];

            for (Text t: value) {
                String[] curr = t.toString().split(" ");
                for (int i = 0; i < numValues - 1; i++) {
                    try {sums[i] += Double.parseDouble(curr[i]);}
                    catch (NumberFormatException nde) {sums[i] += 0.0;}
                    catch (ArrayIndexOutOfBoundsException ae) {break;}
                }
            }

            String finalOutput = Arrays.stream(sums).mapToObj(x -> Double.toString(x / 10000)).collect(Collectors.joining(", "));
            finalOutput.replaceAll(",", "");
            context.write(key, new Text("[" + finalOutput + "]"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "q7");
        job.setJarByClass(CombineFiles.class);

        switch (args[3]) {
            case "1": job.setMapperClass(StartMapper.class); break;
            case "2": job.setMapperClass(PitchMapper.class); break;
            case "3": job.setMapperClass(TimbreMapper.class); break;
            case "4": job.setMapperClass(LMaxMapper.class); break;
            case "5": job.setMapperClass(LMaxTimeMapper.class); break;
            case "6": job.setMapperClass(LStartMapper.class); break;
            default: System.exit(1);
        }

        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
