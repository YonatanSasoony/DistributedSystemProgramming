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


public class Count_Cw1 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text decadeAndBigram, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            String[] values = decadeAndBigram.toString().split("##");
            String decade = values[0];
            String bigram = values[1];
            String w1 = bigram.split(" ")[0];
            context.write(new Text(decade + "##" + w1), occurrences);
            context.write(decadeAndBigram, new LongWritable(0));
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long Cw1 = 0;
        @Override
        // <decade##W1, occ1,...,occK> Or <decade##bigram , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            if(!key.toString().contains(" ")){
                Cw1 = 0;
                for(LongWritable val : value)
                    Cw1 += val.get();
            }else{
                context.write(new Text(key.toString()+"$$Cw1"), new LongWritable(Cw1));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW1 = key.toString().split(" ")[0];
            return decadeAndW1.hashCode() % numPartitions;  // TODO: numPartitions? and hashCode()?
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count C(w1)");
        job.setJarByClass(Count_Cw1.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class); TODO: how to read the new input?
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}