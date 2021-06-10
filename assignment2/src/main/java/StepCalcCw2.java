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

public class StepCalcCw2 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        private static LongWritable zero;

        @Override
        protected void setup(Context context) {
            zero = new LongWritable(0);
        }

        @Override
        // input- <decade##bigram, occ> Or <decade<Ntag>, N>
        public void map(Text key, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            if (key.toString().contains(Defs.NTag)) { // <decade<Ntag>, N>
                return;
            }
            Text decadeAndBigram = key;
            String[] values = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = values[0];
            String bigram = values[1];
            String[] words = bigram.split(Defs.internalBigramDelimiter);
            String w1 = words[0];
            String w2 = words[1];
            context.write(new Text(decade + Defs.decadeBigramDelimiter + w2), occurrences);
            String decadeAndReverseBigram = decade + Defs.decadeBigramDelimiter + w2 + Defs.internalBigramDelimiter + w1;
            context.write(new Text(decadeAndReverseBigram), zero);
        }
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // <decade##W2, occ1,...,occK> Or <decade##W2W1 , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            long Cw2 = 0;
            for(LongWritable val : value) {
                Cw2 += val.get();
            }
            context.write(key, new LongWritable(Cw2));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW2 = key.toString().split(Defs.internalBigramDelimiter)[0];
            return decadeAndW2.hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static long Cw2;

        @Override
        protected void setup(Context context) {
            Cw2 = 0;
        }

        @Override
        // <decade##W2, occ1,...,occK> Or <decade##W2W1 , 0>
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,  InterruptedException {
            if(!key.toString().contains(Defs.internalBigramDelimiter)){
                Cw2 = 0;
                for(LongWritable val : value)
                    Cw2 += val.get();
            }else{
                String[] decadeAndReverseBigram = key.toString().split(Defs.decadeBigramDelimiter);
                String decade = decadeAndReverseBigram[0];
                String reversedBigram = decadeAndReverseBigram[1];
                String[] words = reversedBigram.split(Defs.internalBigramDelimiter);
                String w2 = words[0];
                String w1 = words[1];
                String decadeAndOriginalBigram = decade + Defs.decadeBigramDelimiter + w1 + Defs.internalBigramDelimiter + w2;
                context.write(new Text(decadeAndOriginalBigram+Defs.tagsDelimiter+"Cw2"), new LongWritable(Cw2));
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
        job.setJarByClass(StepCalcCw2.class);
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