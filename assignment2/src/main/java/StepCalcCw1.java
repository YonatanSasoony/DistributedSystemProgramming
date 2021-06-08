import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class StepCalcCw1 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        private static LongWritable zero;

        @Override
        protected void setup(Context context) {
            zero = new LongWritable(0);
        }

        @Override
        public void map(Text key, LongWritable occurrences, Context context) throws IOException, InterruptedException {
            if (key.toString().contains(Defs.NTag)) { // <decade#N, N>
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

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW1 = key.toString().split(Defs.internalBigramDelimiter)[0];
            return decadeAndW1.hashCode() % numPartitions;
        }
    }
}