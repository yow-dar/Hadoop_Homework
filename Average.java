import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Average {

 public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

    private Text word = new Text();
    private MapWritable arry = new MapWritable();

    private int count = 0 ;
    private int sum = 0 ;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
                count += 1;
                sum = sum + Integer.parseInt(tokenizer.nextToken());
        }

    }
    public void cleanup(Context context) throws IOException, InterruptedException{

        arry.put( new IntWritable(0) , new IntWritable(count) ) ;
        arry.put( new IntWritable(1) , new IntWritable(sum) ) ;
        context.write(word,arry);

    }
 }

 public static class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<MapWritable> arry, Context context)  throws IOException, InterruptedException {
        int count_output = 0;
        int sum_output = 0;

        for (MapWritable ar : arry) {
                count_output = count_output + ((IntWritable)ar.get(new IntWritable(0))).get();
                sum_output = sum_output + ((IntWritable)ar.get(new IntWritable(1))).get();
        }

        context.write(key, new DoubleWritable(sum_output/count_output));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Avg");

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setJarByClass(Average.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
   }
}
