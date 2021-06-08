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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.lang.Math;

public class StepCalcNpmi {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, Text> {

        private boolean isCw1w2Tag(Text key) {
            return !key.toString().contains(Defs.tagsDelimiter);
        }

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            if (key.toString().contains(Defs.NTag)) {
                String N = value.toString();
                Text decadeAndNTag = key;
                context.write(decadeAndNTag, new Text(N));
            }
            else if (isCw1w2Tag(key)) {
                Text decadeAndBigram = new Text(key.toString());
                context.write(decadeAndBigram, new Text(value.toString() + Defs.tagsDelimiter+"Cw1w2"));
            } else {
                String decadeAndBigramAndTag = key.toString();
                String[] values = decadeAndBigramAndTag.split(Defs.tagsDelimiter);
                Text decadeAndBigram = new Text(values[0]);
                String tag = values[1];
                Text valueAndTag = new Text(value.toString() + Defs.tagsDelimiter+tag);
                context.write(decadeAndBigram, valueAndTag);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        private long N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = 0;
        }

        @Override
        // <decade#N, N> or <decade##W1W2, [Cw1, Cw2, Cw1w2]>
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if (key.toString().contains(Defs.NTag)) { // <decade#N, N>
                N = Long.parseLong(values.iterator().next().toString());
                return;
            }
            // <decade##W1W2, [Cw1, Cw2, Cw1w2]>
            Text decadeAndBigram = key;
            long Cw1 = 1, Cw2 = 1, Cw1w2 = 1;
            for(Text valueAndTag : values){
                String[] toks = valueAndTag.toString().split(Defs.tagsDelimiter);
                String val = toks[0];
                String tag = toks[1];
                switch (tag){
                    case "Cw1":
                        Cw1 = Long.parseLong(val);
                        break;
                    case "Cw2":
                        Cw2 = Long.parseLong(val);
                        break;
                    case "Cw1w2":
                        Cw1w2 = Long.parseLong(val);
                        break;
                }
            }
            double pmiW1W2 = Math.log(Cw1w2) + Math.log(N) - Math.log(Cw1) - Math.log(Cw2);
            double Pw1w2 = Cw1w2 * 1.0 / N;
            double npmi = pmiW1W2 / (-Math.log(Pw1w2));
            context.write(decadeAndBigram, new DoubleWritable(npmi));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String decade;
            if (key.toString().contains(Defs.NTag)) {
                String decadeAndNTag  = key.toString();
                decade = decadeAndNTag.split(Defs.NTag)[0];
            }else {
                String decadeAndBigram  = key.toString();
                decade = decadeAndBigram.split(Defs.decadeBigramDelimiter)[0];
            }
            return decade.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello StepCalcNpmi main");
        String input1 = args[0];
        String input2 = args[1];
        String input3 = args[2];
        String input4 = args[3];
        String output = args[4];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc npmi");
        job.setJarByClass(StepCalcNpmi.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input3));
        FileInputFormat.addInputPath(job, new Path(input4));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}