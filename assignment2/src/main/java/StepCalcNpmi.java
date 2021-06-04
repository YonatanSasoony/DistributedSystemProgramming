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

        @Override
        public void map(Text decadeAndBigramAndTag, LongWritable value, Context context) throws IOException, InterruptedException {
            // TODO: name
            if (!decadeAndBigramAndTag.toString().contains("@")) {// this is Cw1w2
                Text decadeAndBigram = new Text(decadeAndBigramAndTag.toString());
                context.write(decadeAndBigram, new Text(value.toString() + "@Cw1w2"));
            } else {//check tags
                String[] values = decadeAndBigramAndTag.toString().split("@");
                Text decadeAndBigram = new Text(values[0]);
                String tag = values[1];
                Text valueAndTag = new Text(value.toString() + "@"+tag);
                context.write(decadeAndBigram, valueAndTag);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        @Override
        // <decade##W1W2, [N, Cw1, Cw2, Cw1e2]>
        public void reduce(Text decadeAndBigram, Iterable<Text> valuesAndTags, Context context) throws IOException,  InterruptedException {
            long N = 1, Cw1 = 1, Cw2 = 1, Cw1w2 = 1;
            for(Text valueAndTag : valuesAndTags){
                String[] values = valueAndTag.toString().split("@");
                String val = values[0];
                String tag = values[1];
                switch (tag){
                    case "N":
                        N = Long.parseLong(val);
                        break;
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
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        String input1 = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\N_output\\part-r-00000";
        String input2 = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Cw1w2_output\\part-r-00000";
        String input3 = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Cw1_output\\part-r-00000";
        String input4 = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Cw2_output\\part-r-00000";
        String output = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Npmi_output";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc npmi");
        job.setJarByClass(StepCalcNpmi.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input3));
        FileInputFormat.addInputPath(job, new Path(input4));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}