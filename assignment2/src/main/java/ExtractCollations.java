import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.*;

public class ExtractCollations {
    public static void main(String[] args) {
        if(args == null || args.length != 2) {
            System.out.println("invalid input usage: java -cp ass2.jar ExtractCollations <minPmi> <relMinPmi>");
            return;
        }

        String minNpmi = args[0];
        String relMinNpmi = args[1];

        BasicConfigurator.configure();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion(Regions.US_EAST_1).build();

        // input: data set
        // output: decade##w1w2 -> occurrences = Cw1w2+N
        HadoopJarStepConfig hadoopJarStepCalcCw1Cw2N = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepCalcCw1w2N.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1w2N")
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data",
                        "s3n://dsp-ass2/Cw1w2_N_output");

        // input: decade##w1w2 ->Cw1w2+N
        // output: decade##w1w2 -> Cw1
        HadoopJarStepConfig hadoopJarStepCalcCw1 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepCalcCw1.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1")
                .withArgs("s3n://dsp-ass2/Cw1w2_N_output/part-r-00000",
                        "s3n://dsp-ass2/Cw1_output/");

        // input: decade##w1w2 -> Cw1w2+N
        // output: decade##w1w2 -> Cw2
        HadoopJarStepConfig hadoopJarStepCalcCw2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepCalcCw2.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw2")
                .withArgs("s3n://dsp-ass2/Cw1w2_N_output/part-r-00000",
                        "s3n://dsp-ass2/Cw2_output/");

        // input: decade##w1w2 -> Cw1w2+N
        //        decade##w1w2 -> Cw1
        //        decade##w1w2 -> Cw2
        // output: decade##w1w2 -> npmi
        HadoopJarStepConfig hadoopJarStepCalcNpmi = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepCalcNpmi.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcNpmi")
                .withArgs("s3n://dsp-ass2/Cw1w2_N_output/part-r-00000", "s3n://dsp-ass2/Cw1_output/part-r-00000",
                        "s3n://dsp-ass2/Cw2_output/part-r-00000",
                        "s3n://dsp-ass2/Npmi_output/");

        // input: decade##w1w2 -> npmi
        // output: decade##w1w2 -> npmi (filter collocation)
        HadoopJarStepConfig hadoopJarStepFilterCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepFilterCollocations.jar") // This should be a full map reduce application.
                .withMainClass("StepFilterCollocations")
                .withArgs("s3n://dsp-ass2/Npmi_output/part-r-00000",
                        "s3n://dsp-ass2/Filtered_output/", minNpmi, relMinNpmi);

        // input: decade##w1w2 -> npmi (filter collocation)
        // output: decade##w1w2 -> npmi (sorted collocation)
        HadoopJarStepConfig hadoopJarStepSortCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/StepSortCollocationsDescending.jar") // This should be a full map reduce application.
                .withMainClass("StepSortCollocationsDescending")
                .withArgs("s3n://dsp-ass2/Filtered_output/part-r-00000",
                        "s3n://dsp-ass2/Sorted_output/");

        StepConfig stepConfig1 = new StepConfig()
                .withName("StepCalcCw1w2N")
                .withHadoopJarStep(hadoopJarStepCalcCw1Cw2N)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig2 = new StepConfig()
                .withName("StepCalcCw1")
                .withHadoopJarStep(hadoopJarStepCalcCw1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig3 = new StepConfig()
                .withName("StepCalcCw2")
                .withHadoopJarStep(hadoopJarStepCalcCw2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig4 = new StepConfig()
                .withName("StepCalcNpmi")
                .withHadoopJarStep(hadoopJarStepCalcNpmi)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig5 = new StepConfig()
                .withName("StepFilterCollocations")
                .withHadoopJarStep(hadoopJarStepFilterCollocations)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig6 = new StepConfig()
                .withName("StepSortCollocations")
                .withHadoopJarStep(hadoopJarStepSortCollocations)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M5_XLARGE.toString())
                .withSlaveInstanceType(InstanceType.M5_XLARGE.toString())
                .withHadoopVersion("3.2.1").withEc2KeyName("awsKeyPair")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DspAss2")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3, stepConfig4, stepConfig5, stepConfig6)
                .withLogUri("s3n://dsp-ass2/logs")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-6.2.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        // how to run:
        // in main folder-
        // mvn compile assembly:single
        // cd target
        // java -cp ExtractCollations-1.0-jar-with-dependencies.jar ExtractCollations 0.5 0.2
        // java -cp ExtractCollations.jar ExtractCollations 0.5 0.2
    }
}
