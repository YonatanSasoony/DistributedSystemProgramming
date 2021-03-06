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

public class StepCalcCw1 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        private static LongWritable zero;

        @Override
        protected void setup(Context context) {
            zero = new LongWritable(0);
        }

        @Override
        // input- <decade##bigram, occ> Or <decade<Ntag> , N>
        public void map(Text key, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            if (key.toString().contains(Defs.NTag)) { // <decade<Ntag>, N>
                return;
            }
            Text decadeAndBigram = key;
            String[] values = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = values[0];
            String bigram = values[1];
            String w1 = bigram.split(Defs.internalBigramDelimiter)[0];
            context.write(new Text(decade + Defs.decadeBigramDelimiter + w1), occurrences);
            context.write(decadeAndBigram, zero);
        }
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // <decade##W1, occ1,...,occK> Or <decade##bigram , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            long Cw1 = 0;
            for(LongWritable val : value) {
                Cw1 += val.get();
            }
            context.write(key, new LongWritable(Cw1));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        // <decade##W1, occ1,...,occK> Or <decade##bigram , 0>
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW1 = key.toString().split(Defs.internalBigramDelimiter)[0];
            return decadeAndW1.hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long Cw1;

        @Override
        protected void setup(Context context) {
            Cw1 = 0;
        }

        @Override
        // <decade##W1, occ1,...,occK> Or <decade##bigram , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            if(!key.toString().contains(Defs.internalBigramDelimiter)){
                Cw1 = 0;
                for(LongWritable val : value)
                    Cw1 += val.get();
            }else{
                context.write(new Text(key+Defs.tagsDelimiter+"Cw1"), new LongWritable(Cw1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String jarName = args[0];
        String input = args[1];
        String output = args[2];
        System.out.println("Hello "+jarName+" main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, jarName);
        job.setJarByClass(StepCalcCw1.class);
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