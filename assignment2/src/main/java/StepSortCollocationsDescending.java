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

public class StepSortCollocationsDescending {

    public static class MapperClass extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {

        @Override
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String[] toks = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = toks[0];
            String bigram = toks[1];
            Double negativeDecadeAndNpmi = ((Double.parseDouble(decade) * 1000) + npmi.get()) * - 1.0;

            context.write(new DoubleWritable(negativeDecadeAndNpmi), new Text(bigram));
        }
    }


    public static class ReducerClass extends Reducer<DoubleWritable,Text,Text,Text> {
        private static Double prevDecade;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            prevDecade = -1.0;
        }

        @Override
        //  <decade<>npmi<>bigram , npmi>
        public void reduce(DoubleWritable negativeDecadeAndNpmi, Iterable<Text> bigrams, Context context) throws IOException,  InterruptedException {

            Double decade = negativeDecadeAndNpmi.get() / (-1000.0);
            Double npmi = negativeDecadeAndNpmi.get() - decade;

            if (!decade.equals(prevDecade)) { // split by decades and print titles
                prevDecade = decade;
                context.write(new Text(""), new Text(""));
                context.write(new Text("Decade:"), new Text(decade+"0s"));
            }
            context.write(new Text(bigrams.iterator().next()), new Text(npmi.toString()));
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
        job.setJarByClass(StepSortCollocationsDescending.class);
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

