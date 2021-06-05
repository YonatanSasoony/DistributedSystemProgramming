import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class StepCalcCw1w2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static StopWordsSet s = StopWordsSet.getInstance();
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] lineData = line.toString().split(Defs.lineDataDelimiter);
            try{
                String bigram = lineData[0];
                Integer year = Integer.parseInt(lineData[1]);
                Integer occurrences  = Integer.parseInt(lineData[2]);
                String decade = Integer.toString(year/10);
                Text key = new Text(decade + Defs.decadeBigramDelimiter + bigram);
                String[] words = bigram.split(Defs.internalBigramDelimiter);
                String w1 = words[0];
                String w2 = words[1];
                if(!s.contains(w1) && !s.contains(w2))
                    context.write(key, new LongWritable(occurrences));
            }catch (Exception e){
                return;
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text decadeAndBigram, Iterable<LongWritable> occurrences , Context context) throws IOException,  InterruptedException {
            long totalDecadeOccurrences = 0;
            for(LongWritable occ : occurrences) {
                totalDecadeOccurrences += occ.get();
            }
            context.write(decadeAndBigram, new LongWritable(totalDecadeOccurrences));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
//        String input = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\bigrams.txt";
        String input = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\heb-2gram.txt";
        String output = "C:\\Users\\yc132\\OneDrive\\שולחן העבודה\\AWS\\ASS2\\DistributedSystemProgramming\\assignment2\\src\\main\\java\\Cw1w2_output";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc Cw1w2");
        job.setJarByClass(StepCalcCw1w2.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class); //TODO turn on when using full data set
//        job.setInputFormatClass(TextInputFormat.class); //TODO ?
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}