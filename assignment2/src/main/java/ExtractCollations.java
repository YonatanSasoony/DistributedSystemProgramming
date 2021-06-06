import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimits;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimitsUnitType;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ManagedScalingPolicy;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;


public class ExtractCollations {
    public static void main(String[] args) {
        System.out.println("Hello ExtractCollations main");//TODO remove
        if(args == null || args.length != 2) {
            System.out.println("invalid input usage: java -cp ass2.jar ExtractCollations <minPmi> <relMinPmi>");
            System.out.println("args0:"+args[0]);
            return;
        }
// izhak's
//        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
//                .standard()
//                .withCredentials(credentialsProvider)
//                .withRegion("us-east-1")
//                .build();

// yonatan's
        BasicConfigurator.configure();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder().withRegion(Regions.US_EAST_1).build();

        // input: data set
        // output: decade##w1w2 -> occurrences = Cw1w2
        HadoopJarStepConfig hadoopJarStepCalcCw1Cw2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1w2")
                .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3n://dsp-ass2/Cw1w2_output");
//                .withArgs("s3://dsp-ass2/bigrams.txt", "s3n://dsp-ass2/outputCw1Cw2/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> N
        HadoopJarStepConfig hadoopJarStepCalcN = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcN")
                .withArgs("s3n://dsp-ass2/Cw1w2_output/part-r-00000", "s3n://dsp-ass2/N_output/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> Cw1
        HadoopJarStepConfig hadoopJarStepCalcCw1 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1")
                .withArgs("s3n://dsp-ass2/Cw1w2_output/part-r-00000", "s3n://dsp-ass2/Cw1_output/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> Cw2
        HadoopJarStepConfig hadoopJarStepCalcCw2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw2")
                .withArgs("s3n://dsp-ass2/Cw1w2_output/part-r-00000", "s3n://dsp-ass2/Cw2_output/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        //        decade##w1w2 -> N
        //        decade##w1w2 -> Cw1
        //        decade##w1w2 -> Cw2
        // output: decade##w1w2 -> npmi
        HadoopJarStepConfig hadoopJarStepCalcNpmi = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcNpmi")
                .withArgs("s3n://dsp-ass2/Cw1w2_output/part-r-00000","s3n://dsp-ass2/N_output/part-r-00000",
                          "s3n://dsp-ass2/Cw1_output/part-r-00000", "s3n://dsp-ass2/Cw2_output/part-r-00000",
                          "s3n://dsp-ass2/Npmi_output/");

        // input: decade##w1w2 -> npmi
        // output: decade##w1w2 -> npmi (filter collocation)
        HadoopJarStepConfig hadoopJarStepFilterCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepFilterCollocations") //TODO; check args
                .withArgs(args[0], args[1], "s3n://dsp-ass2/Npmi_output/part-r-00000", "s3n://dsp-ass2/Filtered_output/");

        // input: decade##w1w2 -> npmi (filter collocation)
        // output: decade##w1w2 -> npmi (sorted collocation)
        HadoopJarStepConfig hadoopJarStepSortCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepSortCollocations")
                .withArgs("s3n://dsp-ass2/Filtered_output/part-r-00000", "s3n://dsp-ass2/Sorted_output/");


        StepConfig stepConfig1 = new StepConfig()
                .withName("stepConfig1")
                .withHadoopJarStep(hadoopJarStepCalcCw1Cw2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig2 = new StepConfig()
                .withName("stepConfig2")
                .withHadoopJarStep(hadoopJarStepCalcN)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig3 = new StepConfig()
                .withName("stepConfig3")
                .withHadoopJarStep(hadoopJarStepCalcCw1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig4 = new StepConfig()
                .withName("stepConfig4")
                .withHadoopJarStep(hadoopJarStepCalcCw2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig5 = new StepConfig()
                .withName("stepConfig5")
                .withHadoopJarStep(hadoopJarStepCalcNpmi)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig6 = new StepConfig()
                .withName("stepConfig6")
                .withHadoopJarStep(hadoopJarStepFilterCollocations)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig7 = new StepConfig()
                .withName("stepConfig7")
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
                .withName("DspAss2") //TODO: if not working- put ec2 key name
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3, stepConfig4, stepConfig5, stepConfig6, stepConfig7)
                .withLogUri("s3n://dsp-ass2/logs/")
                .withServiceRole("EMR_DefaultRole")// TODO  params??
                .withJobFlowRole("EMR_EC2_DefaultRole") // TODO  params?? .withReleaseLabel("emr-5.11.0");
                .withReleaseLabel("emr-6.2.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        // how to run:
        // in main folder-
        // mvn clean compile assembly:single
        // cd target
        // java -cp ExtractCollations-1.0-jar-with-dependencies.jar ExtractCollations 0.5 0.2
    }
}
