import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Partitioner;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;

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
        import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;

//package dsp.hadoop.examples;
        import java.io.DataInput;

        import java.io.DataOutput;
        import java.io.IOException;
        import java.text.SimpleDateFormat;
        import java.util.Calendar;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import java.io.IOException;


import java.util.HashMap;
import java.util.Map;
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


public class isCollocations {

    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String decade = decadeAndBigram.toString().split("##")[0];
            context.write(new Text(decade), npmi);
            context.write(decadeAndBigram, npmi);
        }
    }

    public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private static double decadeTotalNpmi  = 0;

        @Override
        // <decade, npmi1....npmiK> Or <decade##W2W1 , npmi>
        public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException,  InterruptedException {
            double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi","1"));
            double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi","1"));
            if(!key.toString().contains("##")){
                decadeTotalNpmi = 0;
                for(DoubleWritable val : value)
                    decadeTotalNpmi += val.get();
            }else{
                double npmi = 1;
                for(DoubleWritable val : value)
                    npmi = val.get();
                if(npmi >= minPmi && npmi / decadeTotalNpmi >= relMinPmi) {
                    context.write(key, new DoubleWritable(npmi));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decade = key.toString().split("##")[0];
            return decade.hashCode() % numPartitions;  // TODO: numPartitions? and hashCode()?
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("minPmi", args[1]);
        conf.set("relMinPmi", args[2]);
        Job job = Job.getInstance(conf, "isCollocations");
        job.setJarByClass(isCollocations.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class); TODO: how to read the new input?
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

