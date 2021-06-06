import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
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

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());



        System.out.println("Creating EMR instance");
        System.out.println("===========================================");
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

//        AWSCredentials credentials = new PropertiesCredentials(...);
//        AmazonElasticMapReduce mapReduce = new
//                AmazonElasticMapReduceClientBuilder.defaultClient(;
//        AWSCredentials credentials_profile = null;
//        try {
//            credentials_profile = new ProfileCredentialsProvider("default").getCredentials(); // specifies any named profile in .aws/credentials as the credentials provider
//        } catch (Exception e) {
//            throw new AmazonClientException(
//                    "Cannot load credentials from .aws/credentials file. Make sure that the credentials file exists and that the profile name is defined within it.", e);
//        }
//        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
//                .withRegion(Regions.US_EAST_1)
//                .build();

        // input: data set
        // output: decade##w1w2 -> occurrences = Cw1w2
        HadoopJarStepConfig hadoopJarStepCalcCw1Cw2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1Cw2")
                //.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3n://dsp-ass2/output1/");
                .withArgs("s3://dsp-ass2/bigrams.txt", "s3n://dsp-ass2/outputCw1Cw2/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> N
        HadoopJarStepConfig hadoopJarStepCalcN = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcN")
                .withArgs("s3n://dsp-ass2/outputCw1Cw2/", "s3n://dsp-ass2/outputN/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> Cw1
        HadoopJarStepConfig hadoopJarStepCalcCw1 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw1")
                .withArgs("s3n://dsp-ass2/outputCw1Cw2/", "s3n://dsp-ass2/outputCw1/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        // output: decade##w1w2 -> Cw2
        HadoopJarStepConfig hadoopJarStepCalcCw2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcCw2")
                .withArgs("s3n://dsp-ass2/outputCw1Cw2/", "s3n://dsp-ass2/outputCw2/");

        // input: decade##w1w2 -> occurrences = Cw1w2
        //        decade##w1w2 -> N
        //        decade##w1w2 -> Cw1
        //        decade##w1w2 -> Cw2
        // output: decade##w1w2 -> npmi
        HadoopJarStepConfig hadoopJarStepCalcNpmi = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepCalcNpmi")
                .withArgs("s3n://dsp-ass2/outputCw1Cw2/","s3n://dsp-ass2/outputN/",
                          "s3n://dsp-ass2/outputCw1/", "s3n://dsp-ass2/outputCw2/",
                          "s3n://dsp-ass2/outputNpmi/");

        // input: decade##w1w2 -> npmi
        // output: decade##w1w2 -> npmi (filter collocation)
        HadoopJarStepConfig hadoopJarStepFilterCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepFilterCollocations " + args[0] + " " + args[1]) //TODO; check args
                .withArgs("s3n://dsp-ass2/outputNpmi/", "s3n://dsp-ass2/outputDspAss2/");

        // input: decade##w1w2 -> npmi (filter collocation)
        // output: decade##w1w2 -> npmi (sorted collocation)
        HadoopJarStepConfig hadoopJarStepSortCollocations = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("StepSortCollocations " + args[0] + " " + args[1])
                .withArgs("s3n://dsp-ass2/outputNpmi/", "s3n://dsp-ass2/outputDspAss2/");


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
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("awsKeyPair")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DspAss2") //TODO: if not working- put ec2 key name
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3, stepConfig4, stepConfig5, stepConfig6, stepConfig7)
                .withLogUri("s3n://dsp-ass2/logs/")
                .withServiceRole("EMR_DefaultRole")// TODO  params??
                .withJobFlowRole("EMR_EC2_DefaultRole"); // TODO  params?? .withReleaseLabel("emr-5.11.0");

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
