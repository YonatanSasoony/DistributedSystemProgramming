import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StepsRunner {


    private static boolean runStepCalcCw1w2(String input, String output) throws Exception {
        System.out.println("Hello StepCalcCw1w2 main");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calc Cw1w2");
        job.setJarByClass(StepCalcCw1w2.class);
        job.setInputFormatClass(SequenceFileInputFormat.class); //TODO turn on when using full data set
        job.setMapperClass(StepCalcCw1w2.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(StepCalcCw1w2.PartitionerClass.class);
        job.setCombinerClass(StepCalcCw1w2.ReducerClass.class);
        job.setReducerClass(StepCalcCw1w2.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepCalcCw1(String input, String output) throws Exception {
        System.out.println("Hello StepCalcCw1 main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc Cw1");
        job.setJarByClass(StepCalcCw1.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(StepCalcCw1.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setCombinerClass(StepCalcCw1.CombinerClass.class);
        job.setPartitionerClass(StepCalcCw1.PartitionerClass.class);
        job.setReducerClass(StepCalcCw1.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepCalcCw2(String input, String output) throws Exception {
        System.out.println("Hello StepCalcCw2 main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc Cw2");
        job.setJarByClass(StepCalcCw2.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(StepCalcCw2.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setCombinerClass(StepCalcCw2.CombinerClass.class);
        job.setPartitionerClass(StepCalcCw2.PartitionerClass.class);
        job.setReducerClass(StepCalcCw2.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepCalcN(String input, String output) throws Exception {
        System.out.println("Hello StepCalcN main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc N");
        job.setJarByClass(StepCalcN.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(StepCalcN.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setCombinerClass(StepCalcN.CombinerClass.class);
        job.setPartitionerClass(StepCalcN.PartitionerClass.class);
        job.setReducerClass(StepCalcN.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepCalcNpmi(String input1, String input2, String input3, String input4,
                                           String output) throws Exception {
        System.out.println("Hello StepCalcNpmi main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "calc npmi");
        job.setJarByClass(StepCalcNpmi.class);
        job.setInputFormatClass(LineToTextAndLongInputFormat.class);
        job.setMapperClass(StepCalcNpmi.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(StepCalcNpmi.PartitionerClass.class);
        job.setReducerClass(StepCalcNpmi.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input3));
        FileInputFormat.addInputPath(job, new Path(input4));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepFilterCollocations(String input, String output,
                                                     String minPmi, String relMinPmi) throws Exception {
        System.out.println("Hello StepCalcFilter main");

        Configuration conf = new Configuration();
        conf.set("minPmi", minPmi);
        conf.set("relMinPmi", relMinPmi);

        Job job = Job.getInstance(conf, "filter collocations");
        job.setJarByClass(StepFilterCollocations.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        job.setMapperClass(StepFilterCollocations.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setCombinerClass(StepFilterCollocations.CombinerClass.class);
        job.setPartitionerClass(StepFilterCollocations.PartitionerClass.class);
        job.setReducerClass(StepFilterCollocations.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    private static boolean runStepSortCollocations(String input, String output) throws Exception {
        System.out.println("Hello StepCalcSort main");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "sort collocations");
        job.setJarByClass(StepSortCollocations.class);
        job.setInputFormatClass(LineToTextAndDoubleInputFormat.class);
        job.setMapperClass(StepSortCollocations.MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
//        job.setPartitionerClass(PartitionerClass.class); // no need
        job.setReducerClass(StepSortCollocations.ReducerClass.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.out.println("Hello StepsRunner main");

            for (String arg : args) {
                System.out.println("arg:" + arg);
            }
            boolean step1 = runStepCalcCw1w2("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data",
                                            "s3n://dsp-ass2/Cw1w2_output");
            if (!step1) {
                System.out.println("Step 1 failed");
                System.exit(1);
            }

            boolean step2 = runStepCalcN("s3n://dsp-ass2/Cw1w2_output/part-r-00000",
                                        "s3n://dsp-ass2/N_output/");
            if (!step2) {
                System.out.println("Step 2 failed");
                System.exit(1);
            }

            boolean step3 = runStepCalcCw1("s3n://dsp-ass2/Cw1w2_output/part-r-00000",
                                           "s3n://dsp-ass2/Cw1_output");
            if (!step3) {
                System.out.println("Step 3 failed");
                System.exit(1);
            }

            boolean step4 = runStepCalcCw2("s3n://dsp-ass2/Cw1w2_output/part-r-00000",
                                           "s3n://dsp-ass2/Cw2_output/part-r-00000");
            if (!step4) {
                System.out.println("Step 4 failed");
                System.exit(1);
            }

            boolean step5 = runStepCalcNpmi("s3n://dsp-ass2/Cw1w2_output/part-r-00000",
                                            "s3n://dsp-ass2/N_output/part-r-00000",
                                            "s3n://dsp-ass2/Cw1_output/part-r-00000",
                                            "s3n://dsp-ass2/Cw2_output/part-r-00000",
                                            "s3n://dsp-ass2/Npmi_output");
            if (!step5) {
                System.out.println("Step 5 failed");
                System.exit(1);
            }

            boolean step6 = runStepFilterCollocations("s3n://dsp-ass2/Npmi_output/part-r-00000",
                                                      "s3n://dsp-ass2/Filtered_output", args[1], args[2]);
            if (!step6) {
                System.out.println("Step 6 failed");
                System.exit(1);
            }

            boolean step7 = runStepSortCollocations("s3n://dsp-ass2/Filtered_output/part-r-00000",
                                                    "s3n://dsp-ass2/Sorted_output");
            if (!step7) {
                System.out.println("Step 8 failed");
                System.exit(1);
            }

            System.out.println("ran all jobs");
        } catch (Exception e) {
            System.out.println("EXCEPTION: "+e);
        }

    }
}
