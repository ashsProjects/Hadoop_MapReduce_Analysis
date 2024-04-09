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

public class Q6 {
    public static class MapperClass extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String columns[] = value.toString().split("\\|");
            String song = columns[39];
            double energy;
            double danceability;
            
            try {danceability = Double.parseDouble(columns[3]);}
            catch (NumberFormatException nfe) {danceability = 0;}

            try {energy = Double.parseDouble(columns[6]);}
            catch (NumberFormatException nfe) {energy = 0;}

            context.write(new Text(song), new DoubleWritable(danceability + energy));
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

        job.setJarByClass(Q6.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q6 sort");
            job2.setJarByClass(Q6.class);
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
