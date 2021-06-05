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

public class StepFilterCollocations {

    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String decade = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter)[0];
            context.write(new Text(decade), npmi);
            context.write(decadeAndBigram, npmi);
        }
    }

    public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private static double decadeTotalNpmi  = 0;

        @Override
        // <decade, npmi1....npmiK> Or <decade##W1W2 , npmi>
        public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException,  InterruptedException {
            double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi","1"));
            double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi","1"));
            if(!key.toString().contains(Defs.decadeBigramDelimiter)){
                decadeTotalNpmi = 0;
                for(DoubleWritable val : value) {
                    decadeTotalNpmi += val.get();
                }
            }else{
                double npmi = 1;
                for(DoubleWritable val : value) {
                    npmi = val.get();
                }
                if(npmi >= minPmi && npmi / decadeTotalNpmi >= relMinPmi) {
                    context.write(key, new DoubleWritable(npmi));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decade = key.toString().split(Defs.decadeBigramDelimiter)[0];
            return decade.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Npmi_output\\part-r-00000";
        String output = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Filtered_output";

        Configuration conf = new Configuration();
        conf.set("minPmi", "0.1"); // TODO replace with args[0] [1]
        conf.set("relMinPmi", "0.1");
        Job job = Job.getInstance(conf, "filter collocations");
        job.setJarByClass(StepFilterCollocations.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input)); //TODO - replace with args[0] IN ALL THE CODE BASE
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

