//package dsp.hadoop.examples;
import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;



import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DistributeBigramPerDecade {

    // <lineId> <line> <> <>
    public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineData = line.toString().split("\t");
            try{
                String bigram = lineData[0];
                Integer year = Integer.parseInt(lineData[1]);
                IntWritable decade = new IntWritable(year/10);
                context.write(decade, new Text(bigram));
            }catch (Exception e){
                return;
            }
        }
    }

    public static class ReducerClass extends Reducer<IntWritable,Text,IntWritable,Text> {
        @Override
        public void reduce(IntWritable decade, Iterable<Text> bigrams, Context context) throws IOException,  InterruptedException {
            for(Text bigram : bigrams)
                context.write(decade, bigram);
        }
    }

    public static class PartitionerClass extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;  // TODO: numPartitions? and hashCode()?
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distribute bigram");
        job.setJarByClass(DistributeBigramPerDecade.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}