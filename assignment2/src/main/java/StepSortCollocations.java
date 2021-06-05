import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class StepSortCollocations {

    public static class MapperClass extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String[] toks = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = toks[0];
            String bigram = toks[1];
            context.write(new Text(decade+Defs.sortDelimiter+npmi.toString()+Defs.sortDelimiter+bigram), npmi);
        }
    }

    public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,Text> {
        private static String prevDecade  = "-1";

        @Override
        //  <decade<>npmi<>bigram , npmi>
        public void reduce(Text decadeNpmiAndBigram, Iterable<DoubleWritable> value, Context context) throws IOException,  InterruptedException {
            String[] toks = decadeNpmiAndBigram.toString().split(Defs.sortDelimiter);
            String decade = toks[0];
            String npmi = toks[1];
            String bigram = toks[2];
            if (!decade.equals(prevDecade)) {
                prevDecade = decade;
                context.write(new Text(""), new Text(""));
                context.write(new Text("Decade:"), new Text(decade));
            }
            context.write(new Text(bigram), new Text(npmi));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Filtered_output\\part-r-00000";
        String output = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Sorted_output";

        Configuration conf = new Configuration();
        conf.set("minPmi", "0.1"); // TODO replace with args[0] [1]
        conf.set("relMinPmi", "0.1");
        Job job = Job.getInstance(conf, "sort collocations");
        job.setJarByClass(StepSortCollocations.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input)); //TODO - replace with args[0] IN ALL THE CODE BASE
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

