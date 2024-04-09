import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombineFiles {
    public static class AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String songID = value.toString().split("\\|")[0];
            context.write(new Text(songID), value);
        }
    }

    public static class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\\|");
            String songID = columns[7];
            String relevantCols = columns[0] + "|" + columns[1] + "|" + columns[2] + "|" +
                columns[3] + "|" + columns[4] + "|" + columns[5] + "|" + columns[6] + "|" +
                columns[8] + "|" + columns[9] + "|" + columns[10] + "|" + columns[11] + "|" + 
                columns[12] + "|" + columns[13];
            
            context.write(new Text(songID), new Text(relevantCols));
        }
    }

    public static class CombineReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String combined = "";
            String s1 = value.iterator().next().toString();
            String s2 = value.iterator().next().toString();

            if (s1.length() > s2.length()) combined += s1 + "|" + s2;
            else combined += s2 + "|" + s1;

            context.write(NullWritable.get(), new Text(combined));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "|");
        Job job = Job.getInstance(conf, "Combine files");
        job.setJarByClass(CombineFiles.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AnalysisMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MetadataMapper.class);
        job.setReducerClass(CombineReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
