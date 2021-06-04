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

//package dsp.hadoop.examples;


public class StepCalcCw2 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {

        @Override
        public void map(Text decadeAndBigram, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            String[] values = decadeAndBigram.toString().split("##");
            String decade = values[0];
            String bigram = values[1];
            String w1 = bigram.split(" ")[0];
            String w2 = bigram.split(" ")[1];
            context.write(new Text(decade + "##" + w2), occurrences);
            String decadeAndReverseBigram = decade + "##" + w2 + " " + w1;
            context.write(new Text(decadeAndReverseBigram), new LongWritable(0));
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long Cw2 = 0;
        @Override
        // <decade##W2, occ1,...,occK> Or <decade##W2W1 , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            if(!key.toString().contains(" ")){
                Cw2 = 0;
                for(LongWritable val : value)
                    Cw2 += val.get();
            }else{
                String[] decadeAndReverseBigram = key.toString().split("##");
                String decade = decadeAndReverseBigram[0];
                String reversedBigram = decadeAndReverseBigram[1];
                String w2 = reversedBigram.split(" ")[0];
                String w1 = reversedBigram.split(" ")[1];
                String decadeAndOriginalBigram = decade + "##" + w1 + " " + w2;
                context.write(new Text(decadeAndOriginalBigram+"$$Cw2"), new LongWritable(Cw2));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW2 = key.toString().split(" ")[0];
            return decadeAndW2.hashCode() % numPartitions;  // TODO: numPartitions? and hashCode()?
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc Cw2");
        job.setJarByClass(StepCalcCw2.class);
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