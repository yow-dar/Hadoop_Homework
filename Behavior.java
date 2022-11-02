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

public class Behavior {

        public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

                private MapWritable map = new MapWritable();
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                        String line = value.toString();
                        StringTokenizer tokenizer = new StringTokenizer(line,"\n");

                        while (tokenizer.hasMoreTokens()) {

                                String[] token = tokenizer.nextToken().trim().split("\t") ;

                                map.put( new IntWritable(0) , new Text( token[1] ) ) ;
                                map.put( new IntWritable(1) , new Text("-1") ) ;
                                context.write(new Text( token[0] ) , map ) ;

                                map.put( new IntWritable(0) , new Text("-1") );
                                map.put(new IntWritable(1) , new Text(token[0]) );
                                context.write(new Text( token[1] ), map );
                        }



                 }
        }

public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {

        public void reduce(Text key, Iterable<MapWritable> arry, Context context) throws IOException, InterruptedException {
                /*
                ArrayListWritable<Text> following = new ArrayListWritable<Text>();
                ArrayListWritable<Text> follower = new ArrayListWritable<Text>();
                */
                String following = new String();
                String follower = new String();
                for( MapWritable  ar : arry ){

                        if( ( ( Text )ar.get( new IntWritable( 1 ) ) ).toString().equals("-1") ){

                                following = following + ( ( Text )ar.get( new IntWritable( 0 ) ) ).toString()+",";
                        }

                        else if( ( ( Text )ar.get( new IntWritable( 0 ) ) ).toString().equals("-1") ){

                                follower = follower + ( ( Text )ar.get( new IntWritable( 1 ) ) ).toString()+",";
                        }

                }

                following = CheckString(following);
                context.write( key , new Text(following) );

                follower = CheckString(follower);
                context.write( key , new Text(follower) ) ;
    }
        public static String CheckString(String str) throws IOException,InterruptedException{

                return (str == null || str.length() == 0)
                      ? "-1"
                      : (str.substring(0,str.length()-1));

        }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf, "Following Behavior");

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setJarByClass(Behavior.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }

}
