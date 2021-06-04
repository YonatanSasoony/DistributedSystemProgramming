//package dsp.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class StepCalcCw1Cw2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static StopWordsSet s = StopWordsSet.getInstance();
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineData = line.toString().split("\t");
            try{
                String bigram = lineData[0];
                Integer year = Integer.parseInt(lineData[1]);
                Integer occurrences  = Integer.parseInt(lineData[2]);
                String decade = Integer.toString(year/10);
                Text key = new Text(decade + "##" + bigram);
                String w1 = bigram.split(" ")[0];
                String w2 = bigram.split(" ")[1];
                if(!s.contains(w1) && !s.contains(w2))
                    context.write(key, new LongWritable(occurrences));
            }catch (Exception e){
                return;
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text decadeAndBigram, Iterable<LongWritable> occurrences , Context context) throws IOException,  InterruptedException {
            for(LongWritable occ : occurrences)
                context.write(decadeAndBigram, occ);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;  // TODO: numPartitions? and hashCode()?
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");//TODO remove
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc Cw1Cw2");
        job.setJarByClass(StepCalcCw1Cw2.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class); TODO turn on when using full data set
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}