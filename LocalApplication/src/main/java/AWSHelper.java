import com.google.gson.JsonObject;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.IamClientBuilder;
import software.amazon.awssdk.services.iam.model.CreateInstanceProfileRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
//import software.amazon.awssdk.services.iam.*;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

//TODO: catch exceptions
public class AWSHelper {
    private static Ec2Client ec2 = Ec2Client.create();
    private static SqsClient sqs = SqsClient.builder().region(Defs.REGION).build();
    private static S3Client s3 = S3Client.builder().region(Defs.REGION).build();
    private static final String amiId = "ami-0a5c418d2c4ba6690"; //TODO: CHECK IF GOOD AMI- NEED TO INSTALL SOMETHING ELSE?
    private static final String test_amiId = "ami-0d7c811a290cfa0c7"; //TODO: CHECK IF GOOD AMI- NEED TO INSTALL SOMETHING ELSE?

    //TODO: generailize scripts?
    private final static String credentials =
            "[default]\n" +
            "aws_access_key_id=ASIAV4DOLN6KAVL2AI5K\n" +
            "aws_secret_access_key=Y6csHscixhO9wXtBLrDvrdsc3WA7BPexDywOX7ca\n" +
            "aws_session_token=IQoJb3JpZ2luX2VjEGAaCXVzLXdlc3QtMiJHMEUCIQDbVKHvaxuqL3Dnm8o4bkfNrLp3maViosZfPe2nFayKIAIgdkM2jMOtUDt6JxeYMXRcHcOtx/a3x6LAKnoafGbW5H4qtgIIuf//////////ARAAGgw0MDM5NTgzNjIwMDQiDKwrEFhi+afoudV08iqKAtdTzreAL2n8oxi01NIl4N6DGAU5R6dL/tiYie9ijMD/vGFE4c4J2pRcarrplnyQ9/Z0sTUraJ2+984oLUMYIoiye4adYfH3KlV0tfT2aYCvIOpFnPebXEl2XIYxNLYwkNnfCFx5XXQ5O9ms/+yo0aNBVwFJTfdmJjjfjwIkUmO7affy+x3SpIaed7NPa4QFdgVIqBxGUBTEisCfSZNpeOVWFEn1qC2KoDCqHYjURh+Md12iZQJDeOvMd/oaGaEOTDB00dL+2lK+DGOcxDEoeui68XqrzC14YLPFiWdlAJ4EOwE7S9RUwtU6dHXfnSn94Ec2zQAei6ktf5l/d9u/5bw8QZoc+Z0zxdTfMJP3z4MGOp0BlKjoUtIcwyYVV9n7IGkvz7PdQCYJzI7cx7I3VG9YwZjoEd47pvTKAmwktMuT3Jb+CoyZfZtkuzAanlx244ZH7d5jNQL0wbOnWjjIMupb/ZxndDr+/wlzeKUvn21nVcFLcZOp8OWuLgKfqYUgC18vAmKp2jYGPhwuSqJ8ksCz6jWvwIQcvva51UPoj+ciW33QtE08zXT4ZWJ4Tr4C1g==";
    private static final String managerScript =
            "#cloud-boothook\n"+
            "#!/bin/bash\n\n"+
            "rm ~/.aws/credentials\n"+
            "echo [default] >> ~/.aws/credentials\n"+
            "echo aws_access_key_id=ASIAV4DOLN6KAVL2AI5K >> ~/.aws/credentials\n"+
            "echo aws_secret_access_key=Y6csHscixhO9wXtBLrDvrdsc3WA7BPexDywOX7ca >> ~/.aws/credentials\n"+
            "echo aws_session_token=IQoJb3JpZ2luX2VjEGAaCXVzLXdlc3QtMiJHMEUCIQDbVKHvaxuqL3Dnm8o4bkfNrLp3maViosZfPe2nFayKIAIgdkM2jMOtUDt6JxeYMXRcHcOtx/a3x6LAKnoafGbW5H4qtgIIuf//////////ARAAGgw0MDM5NTgzNjIwMDQiDKwrEFhi+afoudV08iqKAtdTzreAL2n8oxi01NIl4N6DGAU5R6dL/tiYie9ijMD/vGFE4c4J2pRcarrplnyQ9/Z0sTUraJ2+984oLUMYIoiye4adYfH3KlV0tfT2aYCvIOpFnPebXEl2XIYxNLYwkNnfCFx5XXQ5O9ms/+yo0aNBVwFJTfdmJjjfjwIkUmO7affy+x3SpIaed7NPa4QFdgVIqBxGUBTEisCfSZNpeOVWFEn1qC2KoDCqHYjURh+Md12iZQJDeOvMd/oaGaEOTDB00dL+2lK+DGOcxDEoeui68XqrzC14YLPFiWdlAJ4EOwE7S9RUwtU6dHXfnSn94Ec2zQAei6ktf5l/d9u/5bw8QZoc+Z0zxdTfMJP3z4MGOp0BlKjoUtIcwyYVV9n7IGkvz7PdQCYJzI7cx7I3VG9YwZjoEd47pvTKAmwktMuT3Jb+CoyZfZtkuzAanlx244ZH7d5jNQL0wbOnWjjIMupb/ZxndDr+/wlzeKUvn21nVcFLcZOp8OWuLgKfqYUgC18vAmKp2jYGPhwuSqJ8ksCz6jWvwIQcvva51UPoj+ciW33QtE08zXT4ZWJ4Tr4C1g== >> ~/.aws/credentials\n"+
            "aws s3 cp s3://assignment1-pre-uploaded-jar/Manager.jar Manager.jar\n"+
            "java -jar Manager.jar";

    private static final String workerScript =
            "#cloud-boothook\n"+
            "#!/bin/bash\n"+
            "aws s3 cp s3://assignment1-pre-uploaded-jar/sarcasm.jar sarcasm.jar\n"+
            "java -jar sarcasm.jar Worker";

    private static final String sh  =
            "#!/bin/bash\n\n" +
//            "echo export AWS_DEFAULT_REGION=\"us-east-1\" >> /etc/profile\n" +
            "cd ~\n" +
            "cd DSP\n" +
            "java -jar test.jar\n";

//    private static final String arn = "arn";
    private static final String arn = "arn:aws:iam::403958362004:instance-profile/EMR_EC2_DefaultRole";

    //EC2
    public static void runManager() {
        boolean isManager = false;
        // get instances
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                List<Tag> tags = instance.tags();
                for (Tag tag : tags) {
                    if (tag.equals(Defs.MANAGER_TAG) && instance.state().name() != InstanceStateName.TERMINATED) {
                        if (instance.state().name() != InstanceStateName.RUNNING &&
                                instance.state().name() != InstanceStateName.PENDING) {
                            ec2.startInstances(StartInstancesRequest.builder().instanceIds(id).build());
                        }
                        isManager = true;
                    }
                }
            }
        }
        if (!isManager) {

            createEC2Instance(test_amiId, sh, arn, Defs.MANAGER_TAG);
        }
    }

//    private static void createRole() {
//        IamClient iam = IamClient.builder()
//                .region(Defs.REGION)
//                .build();
//        JsonObject jsonRole;
//        CreateRoleRequest roleRequest = CreateRoleRequest.builder()
//                .roleName("")
//                .assumeRolePolicyDocument(jsonRole.getAsString())
//                .build();
//        iam.createRole(roleRequest);
//    }

    public static void createWorkerInstance() {
        createEC2Instance(amiId, workerScript, arn, Defs.WORKER_TAG);
    }

    private static void createEC2Instance(String amiId, String script, String arn, Tag tag) {
        byte[] encoded = {0};
        try {
            encoded = Files.readAllBytes(Paths.get("manager.sh"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(arn)
                        .build())
                .userData(Base64.getEncoder().encodeToString(encoded))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        createTagForInstance(instanceId, tag);
    }

    private static void createTagForInstance(String id, Tag tag) {
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(id)
                .tags(tag)
                .build();
        try {
            ec2.createTags(tagRequest);
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    // SQS
    public static void initQueues() {
        initQueue(Defs.MANAGER_REQUEST_QUEUE_NAME);
        initQueue(Defs.MANAGER_RESPONSE_QUEUE_NAME);
        initQueue(Defs.WORKER_REQUEST_QUEUE_NAME);
        initQueue(Defs.WORKER_RESPONSE_QUEUE_NAME);
    }
    private static void initQueue(String name) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(name)
                    .build();
            sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
        }
    }
    public static String queueUrl(String name) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(name)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    public static void sendMessages(String queueName, List<String> bodies) {
        for (String body : bodies) {
            sendMessage(queueUrl(queueName), body);
        }
    }

    public static void sendMessage(String queueName, String body) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .messageBody(body)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    public static List<Message> receiveMessages(String queueName) { // TODO: MAKE BLOCKING?
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .waitTimeSeconds(15)
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    public static Message receiveSingleMessage(String queueName) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .maxNumberOfMessages(1)
                .visibilityTimeout(60)
                .waitTimeSeconds(15)
                .build();

        return sqs.receiveMessage(receiveRequest).messages().get(0);
    }

    public static void deleteMessages(String queueName, List<Message> messages) {
        for (Message msg : messages) {
            deleteMessage(queueName, msg);
        }
    }

    public static void deleteMessage(String queueName, Message msg) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    // S3
    public static String createBucket(String id) {
        String bucket = "bucket" + id;
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .build());
        return bucket;
    }

    public static void uploadFileTOS3(String bucket, String key, String dataPath) {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(new File(dataPath)));
    }

    public static void uploadContentToS3(String bucket, String key, String content) {
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromString(content));
    }

    public static InputStream downloadFromS3(String bucket, String key) {
        return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toBytes()).asInputStream();
    }
}
