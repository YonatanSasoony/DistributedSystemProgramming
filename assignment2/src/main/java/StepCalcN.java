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

public class StepCalcN {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        private static LongWritable zero;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            zero = new LongWritable(0);
        }
        @Override
        public void map(Text decadeAndBigram, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            String decade = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter)[0];
            context.write(new Text(decade), occurrences);
            context.write(decadeAndBigram, zero);
        }
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // < decade##bigram , 0> Or <decade, occ>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            long N = 0;
            for(LongWritable val : value) {
                N += val.get();
            }
            context.write(key, new LongWritable(N));
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private long N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = 0;
        }

        @Override
        // < decade##bigram , 0> Or <decade, occ>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            if(!key.toString().contains(Defs.decadeBigramDelimiter)){
                N = 0;
                for(LongWritable val : value) {
                    N += val.get();
                }
            }else{
                context.write(new Text(key+Defs.tagsDelimiter+"N"), new LongWritable(N));
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
        System.out.println("Hello StepCalcN main");
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc N");
        job.setJarByClass(StepCalcN.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setCombinerClass(CombinerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}