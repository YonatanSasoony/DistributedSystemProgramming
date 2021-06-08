import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class StepCalcCw2 {

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

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decadeAndW2 = key.toString().split(Defs.internalBigramDelimiter)[0];
            return decadeAndW2.hashCode() % numPartitions;
        }
    }
}