import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
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

        if(args == null || args.length != 3)
            System.out.println("invalid input");


//        AWSCredentials credentials = new PropertiesCredentials(...);
//        AmazonElasticMapReduce mapReduce = new
//                AmazonElasticMapReduceClientBuilder.defaultClient(;
        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials(); // specifies any named profile in .aws/credentials as the credentials provider
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and that the profile name is defined within it.",
                    e);
        }
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_EAST_1)
                .build();

        // input: data set
        // output: decade##w1w2 -> occurrences = Cw1w2
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("DistributeBigramPerDecade2")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data", "s3n://DspAss2/output1/");

        // input: decade##w1w2 -> occurrences
        // output: decade##w1w2 -> N
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("CountBigramPerDecade2")
                .withArgs("s3n://DspAss2/output1/", "s3n://DspAss2/outputN/");

        // input: decade##w1w2 -> occurrences
        // output: decade##w1w2 -> Cw1
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("Count_Cw1")
                .withArgs("s3n://DspAss2/output1/", "s3n://DspAss2/outputCw1/");

        // input: decade##w1w2 -> occurrences
        // output: decade##w1w2 -> Cw3
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("Count_Cw2")
                .withArgs("s3n://DspAss2/output1/", "s3n://DspAss2/outputCw2/");

        // input: decade##w1w2 -> occurrences
        //        decade##w1w2 -> N
        //        decade##w1w2 -> Cw1
        //        decade##w1w2 -> Cw2
        // output: decade##w1w2 -> npmi
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("Npmi")
                .withArgs("s3n://DspAss2/output1/","s3n://DspAss2/outputN/",
                          "s3n://DspAss2/outputCw1/", "s3n://DspAss2/outputCw2/",
                          "s3n://DspAss2/outputNpmi/");

        // input: decade##w1w2 -> npmi
        // output: decade##w1w2 -> npmi (filter collocation)
        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
                .withJar("s3n://DspAss2/ExtractCollations.jar") // This should be a full map reduce application.
                .withMainClass("isCollocations " + args[1] + " " + args[2])
                .withArgs("s3n://DspAss2/outputNpmi/", "s3n://DspAss2/outputDspAss2/");


        StepConfig stepConfig1 = new StepConfig()
                .withName("stepConfig1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig2 = new StepConfig()
                .withName("stepConfig2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig3 = new StepConfig()
                .withName("stepConfig3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig4 = new StepConfig()
                .withName("stepConfig4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig5 = new StepConfig()
                .withName("stepConfig5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig6 = new StepConfig()
                .withName("stepConfig6")
                .withHadoopJarStep(hadoopJarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DspAss2")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3, stepConfig4, stepConfig5, stepConfig6)
                .withLogUri("s3n://DspAss2/logs/"); // TODO more params
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}
