import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class StepCalcCw1w2N {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static StopWords stopWords = StopWords.getInstance();
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineData = line.toString().split(Defs.lineDataDelimiter);
            String bigram = lineData[0];
            Integer year = Integer.parseInt(lineData[1]);
            Integer occurrences = Integer.parseInt(lineData[2]);
            String decade = Integer.toString(year / 10);
            Text decadeAndBigram = new Text(decade + Defs.decadeBigramDelimiter + bigram);
            String[] words = bigram.split(Defs.internalBigramDelimiter);
            if (words.length == 2 && !stopWords.contains(words)) {
                context.write(decadeAndBigram, new LongWritable(occurrences));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private long N;
        private String prevDecade;

        @Override
        protected void setup(Context context) {
            N = 0;
            prevDecade = "-1";
        }

        @Override
        public void reduce(Text decadeAndBigram, Iterable<LongWritable> occurrences , Context context) throws IOException,  InterruptedException {
            String decade = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter)[0];
            long totalDecadeOccurrences = occurrences.iterator().next().get();

            context.write(decadeAndBigram, new LongWritable(totalDecadeOccurrences));
            if (prevDecade.equals(decade)) {
                N += totalDecadeOccurrences;
            } else { // write N
                prevDecade = decade;
                context.write(new Text(decade+Defs.NTag), new LongWritable(N));
                N = totalDecadeOccurrences;
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text decadeAndBigram, LongWritable value, int numPartitions) {
            String decade = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter)[0];
            return decade.hashCode() % numPartitions;
        }
    }
}