import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3 {
    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\|");

            String term = data[41].split(" ")[0];
            String songDetails = data[39] + "|" + data[13] + "|" + data[14] + "|" + data[3] + "|" +
                data[4] + "|" + data[10] + "|" + data[6] + "|" + data[7] + "|" + data[9] + "|" +
                data[5] + "|" + data[12] + "|" + term;
            
            Double hotness;
            try {hotness = Double.parseDouble(data[1]);}
            catch (NumberFormatException nfe) {hotness = 0.0;}

            result.set(hotness);
            context.write(new Text(songDetails), result);
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {       
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            DoubleWritable hotness = value.iterator().next();
            context.write(key, hotness);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "q3");
        job.setJarByClass(Q3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q3 sort");
            job2.setJarByClass(Q3.class);
            job2.setMapperClass(DoubleSorter.SortMapper.class);
            job2.setReducerClass(DoubleSorter.SortReducer.class);
            job2.setOutputKeyClass(DoubleWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}