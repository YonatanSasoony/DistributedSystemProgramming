import com.google.gson.JsonObject;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.IamClientBuilder;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
//import software.amazon.awssdk.services.iam.*;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

//TODO: catch exceptions
public class AWSHelper {
    private static Ec2Client ec2 = Ec2Client.create();
    private static SqsClient sqs = SqsClient.builder().region(Defs.REGION).build();
    private static S3Client s3 = S3Client.builder().region(Defs.REGION).build();

    private static final String amiId = "ami-0a92c388d914cf40c";
    private static final String role = "awsAdminRole";
    private static final String keyName = "awsKeyPair";
    private static final String securityGroupIdYOSSY = "sg-ff035dfe";
    private static final String securityGroupIdYONI = "sg-cb94caca";

    private static final InstanceType managerType = InstanceType.T2_MICRO;
    private static final InstanceType workerType = InstanceType.T2_MEDIUM;

    private static final String managerScript =
            "#!/bin/bash\n" +
                    "wget https://assignment1-pre-uploaded-jar.s3.amazonaws.com/Manager.jar\n" +
                    "java -jar Manager.jar\n";

    private static final String workerScript =
            "#!/bin/bash\n" +
                    "wget https://assignment1-pre-uploaded-jar.s3.amazonaws.com/Worker.jar\n" +
                    "java -jar Worker.jar\n";

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
            createManagerInstance();
        }
    }

    public static void createManagerInstance() {
        createEC2Instance(amiId, managerType, managerScript, Defs.MANAGER_TAG);
    }

    public static void createWorkerInstance() {
        createEC2Instance(amiId, workerType, workerScript, Defs.WORKER_TAG);
    }

    public static void createWorkerInstances(int n) {
        for (int i = 0; i < n; i++) {
            createWorkerInstance();
        }
    }

    private static void createEC2Instance(String amiId, InstanceType type, String userData, Tag tag) {
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        IamInstanceProfileSpecification iamSpec = IamInstanceProfileSpecification.builder()
                .name(role)
                .build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(type)
                .imageId(amiId)
                .minCount(1)
                .maxCount(1)
                .keyName(keyName)
                .securityGroupIds(securityGroupIdYONI)
                .iamInstanceProfile(iamSpec)
                .userData(base64UserData)
                .build();

        RunInstancesResponse runResponse = ec2.runInstances(runRequest);
        String instanceId = runResponse.instances().get(0).instanceId();
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

    public static int activeWorkers() {
        int activeWorkers = 0;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                List<Tag> tags = instance.tags();
                for (Tag tag : tags) {
                    if (tag.equals(Defs.WORKER_TAG) && (instance.state().name() == InstanceStateName.RUNNING
                            || instance.state().name() == InstanceStateName.PENDING)) {
                        activeWorkers++;
                    }
                }
            }
        }
        return activeWorkers;
    }

    public static void shutdownWorkers() {
        List<String> ids = new ArrayList<>();
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                List<Tag> tags = instance.tags();
                for (Tag tag : tags) {
                    if (tag.equals(Defs.WORKER_TAG) && (instance.state().name() == InstanceStateName.RUNNING
                            || instance.state().name() == InstanceStateName.PENDING)) {
                        ids.add(id);
                    }
                }
            }
        }
        if (!ids.isEmpty()) {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(ids)
                    .build();
            ec2.terminateInstances(request);
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
            sendMessage(queueName, body);
        }
    }

    public static void sendMessage(String queueName, String body) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .messageBody(body)
//                .delaySeconds(5)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    public static void sendMessageWithDelay(String queueName, String body, int delay) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .messageBody(body)
                .delaySeconds(delay)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    public static List<Message> receiveMessages(String queueName) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .waitTimeSeconds(5) // 0 is short polling, 1-20 is long polling
                .build();
        while (true) {
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            try {
                if (!messages.isEmpty()) {
                    return messages;
                } else {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static Message receiveSingleMessage(String queueName) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl(queueName))
                .maxNumberOfMessages(1)
                .visibilityTimeout(60)
                .waitTimeSeconds(5) // 0 is short polling, 1-20 is long polling
                .build();

        while (true) {
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            try {
                if (!messages.isEmpty()) {
                    return messages.get(0);
                } else {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    public static void deleteS3Bucket(String bucket, List<String> keys) {
        for (String key : keys) {
            deleteObjectFromBucket(bucket, key);
        }
        DeleteBucketRequest request = DeleteBucketRequest.builder()
                .bucket(bucket)
                .build();
        s3.deleteBucket(request);
    }

    private static void deleteObjectFromBucket(String bucket, String key) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3.deleteObject(request);
    }
}
