import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
                context.write(new Text(decade), new LongWritable(occurrences));
            }
        }
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // <decade#bigram, {occ}>, <decade, {occ1, .. occK}>
        public void reduce(Text key, Iterable<LongWritable> occurrences, Context context) throws IOException, InterruptedException {
            long totalOccurrences = 0;
            for (LongWritable occurrence: occurrences) {
                totalOccurrences += occurrence.get();
            }
            context.write(key, new LongWritable(totalOccurrences));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        // <decade#bigram, {occ}>, <decade, {occ1, .. occK}>
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        // <decade#bigram, {occ}>, <decade, {occ1, .. occK}>
        public void reduce(Text key, Iterable<LongWritable> occurrences , Context context) throws IOException,  InterruptedException {
            long totalOccurrences = 0;
            for (LongWritable occurrence: occurrences) {
                totalOccurrences += occurrence.get();
            }

            if (key.toString().contains(Defs.decadeBigramDelimiter)) { // <decade#bigram, {occ}
                context.write(key, new LongWritable(totalOccurrences));
            } else { // <decade, {occ1, .. occK}>
                context.write(new Text(key +Defs.NTag), new LongWritable(totalOccurrences));
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
        job.setJarByClass(StepCalcCw1w2N.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
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