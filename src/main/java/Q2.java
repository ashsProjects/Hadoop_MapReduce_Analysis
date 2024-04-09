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

public class Q2 {
    public static class MapperClass extends Mapper<Object, Text, Text, CountAverageTuple> {
        private CountAverageTuple outCountAverage = new CountAverageTuple();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] metadata = value.toString().split("\\|");
            String loudness = metadata[9];
            String artist = metadata[38];

            outCountAverage.setCount(1);
            outCountAverage.setAverage(Double.parseDouble(loudness));

            context.write(new Text(artist), outCountAverage);
        }
    }

    public static class ReducerClass extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple> {
        private CountAverageTuple result = new CountAverageTuple();
        @Override
        protected void reduce(Text key, Iterable<CountAverageTuple> value, Context context) throws IOException, InterruptedException {
            double sum = 0; int count = 0;
            
            for (CountAverageTuple val: value) {
                sum += val.getCount() * val.getAverage();
                count += val.getCount();
            }

            result.setCount(count);
            result.setAverage(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "q2");

        job.setJarByClass(Q2.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CountAverageTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q2 sort");
            job2.setJarByClass(Q2.class);
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
