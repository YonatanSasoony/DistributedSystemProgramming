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
        // input - <decade#bigram, npmi>
        public void map(Text decadeAndBigram, DoubleWritable npmi, Context context) throws IOException, InterruptedException {
            String[] toks = decadeAndBigram.toString().split(Defs.decadeBigramDelimiter);
            String decade = toks[0];
            String bigram = toks[1];
            Double negativeDecadeAndNpmi = ((Double.parseDouble(decade) * 1000) + npmi.get()) * - 1.0;

            context.write(new DoubleWritable(negativeDecadeAndNpmi), new Text(bigram));
        }
    }

    public static class ReducerClass extends Reducer<DoubleWritable,Text,Text,Text> {
        private static int prevDecade;

        @Override
        protected void setup(Context context) {
            prevDecade = -1;
        }

        @Override
        //  < -(decade * 1000) + npmi), bigram>
        public void reduce(DoubleWritable negativeDecadeAndNpmi, Iterable<Text> bigrams, Context context) throws IOException,  InterruptedException {

            double positiveDecadeAndNpmi = negativeDecadeAndNpmi.get() *(-1);
            int decade =   ((int)positiveDecadeAndNpmi) / 1000;
            Double npmi = positiveDecadeAndNpmi - (decade * 1000);

            if (decade != prevDecade) { // split by decades and print titles
                prevDecade = decade;
                context.write(new Text(""), new Text(""));
                context.write(new Text("Decade:"), new Text(decade+"0s"));
            }
            context.write(new Text(bigrams.iterator().next()), new Text(npmi.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        String jarName = args[0];
        String input = args[0];
        String output = args[1];
        System.out.println("Hello "+jarName+" main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, jarName);
        job.setJarByClass(StepSortCollocationsDescending.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

