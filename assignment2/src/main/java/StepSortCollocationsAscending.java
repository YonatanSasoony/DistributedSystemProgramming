import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class StepSortCollocationsAscending {

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
        private static String prevDecade;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            prevDecade = "-1";
        }

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
                context.write(new Text("Decade:"), new Text(decade+"0s"));
            }
            context.write(new Text(bigram), new Text(npmi));
        }
    }

    // We set the number of reducer tasks to 1, no need a partitioner.
//    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
//        @Override
//        public int getPartition(Text key, LongWritable value, int numPartitions) {
//            return 0;
//        }
//    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello StepCalcSort main");
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "sort collocations");
        job.setJarByClass(StepSortCollocationsAscending.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
//        job.setPartitionerClass(PartitionerClass.class); // no need
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

