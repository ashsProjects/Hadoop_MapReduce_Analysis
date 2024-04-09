import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q10 {
    public static class MapperClass extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\\|");
            String artistSong = columns[38] + "|" + columns[39];
            
            int year;
            try {year = Integer.parseInt(columns[44]);}
            catch (NumberFormatException nfe) {year = 0;}

            context.write(new IntWritable(year), new Text(artistSong));
        }
    }

    public static class ArtistCountMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int year = Integer.parseInt(value.toString().split("\t")[0]);
            String data = value.toString().split("\t")[1];
            context.write(new IntWritable(year), new Text(data));
        }
    }

    public static class ReducerClass extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Map<String, Tuple> artistSongs = new HashMap<>();

            for (Text s: value) {
                String artist = ""; String song = "";
                try {
                    artist = s.toString().split("\\|")[0];
                    song = s.toString().split("\\|")[1];
                } catch (ArrayIndexOutOfBoundsException e) {
                    continue;
                }

                if (artistSongs.containsKey(artist)) {
                    Tuple temp = artistSongs.get(artist);
                    temp.increment();
                    temp.addToList(song);
                }
                else {
                    Tuple tuple = new Tuple();
                    tuple.addToList(song);
                    tuple.increment();
                    artistSongs.put(artist, tuple);
                }
            }

            for (Map.Entry<String, Tuple> val: artistSongs.entrySet()) {
                String builder = val.getKey() + "|" + val.getValue().toString();
                context.write(key, new Text(builder));
            }
        }
    }

    public static class ArtistCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int max = 0;
            String data = "";

            for (Text t: value) {
                int count = 0;
                count = Integer.parseInt(t.toString().split("\\|")[1]);

                if (count > max) {
                    max = count;
                    data = t.toString();
                }
            }
            context.write(key, new Text(data));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "q10 all artists");
        job.setJarByClass(Q10.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "q10 max releases by year");
            job2.setJarByClass(Q10.class);
            job2.setMapperClass(ArtistCountMapper.class);
            job2.setReducerClass(ArtistCountReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

class Tuple {
    private List<String> values;
    private int count;

    public Tuple() {
        values = new ArrayList<>();
        count = 0;
    }

    public void increment() {
        this.count++;
    }

    public int getCount() {
        return this.count;
    }

    public void addToList(String value) {
        this.values.add(value);
    }

    public List<String> getValues() {
        return this.values;
    }

    @Override
    public String toString() {
        String builder = this.count + "|";
        builder += Arrays.toString(this.values.toArray());
        return builder;
    }
}
