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

public class Q5 {
    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String columns[] = value.toString().split("\\|");
            String song = columns[39];
            double duration;
            try {duration = Double.parseDouble(columns[4]);}
            catch (NumberFormatException nfe) {duration = 0;}
            context.write(new Text(song), new DoubleWritable(duration));
        }
    }

    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            DoubleWritable duration = value.iterator().next();
            context.write(key, duration);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(Q5.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q5 sort");
            job2.setJarByClass(Q5.class);
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
