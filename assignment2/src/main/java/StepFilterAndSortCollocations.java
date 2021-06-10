import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class StepFilterAndSortCollocations {

    public static class MapperClass extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {

        @Override
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String[] toks = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = toks[0];
            String bigram = toks[1];
            Double negativeDecade = ((Double.parseDouble(decade) * 1000.0) + 999.0) * (-1.0);
            Double negativeDecadeAndNpmi = ((Double.parseDouble(decade) * 1000.0) + 500.0 + npmi.get()) * (-1.0);
            context.write(new DoubleWritable(negativeDecade), new Text(npmi.toString()));
            context.write(new DoubleWritable(negativeDecadeAndNpmi), new Text(bigram));
        }
    }

    public static class CombinerClass extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        @Override
        // <-((decade*1000) + 999), npmi1....npmiK> Or < -(decade * 1000) + 500 + npmi), bigram>
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int magicNumber = ((int)(key.get() * (-1))) % 1000;
            if (magicNumber == 999) { // <-((decade*1000) + 999), npmi1....npmiK>
                Double decadeTotalNpmi = 0.0;
                for(Text value : values) {
                    double npmi = Double.parseDouble(value.toString());
                    decadeTotalNpmi += npmi;
                }
                context.write(key, new Text(decadeTotalNpmi.toString()));
            }
            else { // < -(decade * 1000) + 500 + npmi), bigram>
                context.write(key, values.iterator().next());
            }
        }
    }

    public static class ReducerClass extends Reducer<DoubleWritable, Text, Text, Text> {
        private static double decadeTotalNpmi;

        @Override
        protected void setup(Context context) {
            decadeTotalNpmi = 0;
        }

        //we did not implemented a Combiner class this time, because we cannot promise that the mapper or the combiner
        //will receive all the relevant information/params for calculating the npmi
        @Override
        // <-((decade*1000) + 999), npmi1....npmiK> Or < -(decade * 1000) + 500 + npmi), bigram>
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int magicNumber = ((int)(key.get() * (-1))) % 1000;
            Integer decade = ((int)(key.get() * (-1))) / 1000;
            if (magicNumber == 999) { // <-((decade*1000) + 999), npmi1....npmiK>
                decadeTotalNpmi = 0.0;
                for(Text val : values) {
                    double npmiTemp = Double.parseDouble(val.toString());
                    decadeTotalNpmi += npmiTemp;
                }
                context.write(new Text(""), new Text(""));
                context.write(new Text("Decade:"), new Text(decade+"0s"));
            } else {// < -(decade * 1000) + 500 + npmi), bigram>
                double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi","1"));
                double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi","1"));;
                Double npmi = (key.get() * (-1)) - 500 - (decade * 1000);
                if(npmi >= minPmi && npmi / decadeTotalNpmi >= relMinPmi) {
                    Text bigram = values.iterator().next();
                    context.write(bigram, new Text(npmi.toString()));
                }
            }
        }
    }

    // We set the number of reducer tasks to 1, no need a partitioner.
//    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
//        @Override
//        public int getPartition(Text key, LongWritable value, int numPartitions) {
//            String decade = key.toString().split(Defs.decadeBigramDelimiter)[0];
//            return decade.hashCode() % numPartitions;
//        }
//    }
}

