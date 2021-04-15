import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

//TODO: catch exceptions
public class AWSHelper {
    private static Ec2Client ec2 = Ec2Client.create();
    private static SqsClient sqs = SqsClient.builder().region(Defs.REGION).build();
    private static S3Client s3 = S3Client.builder().region(Defs.REGION).build();
    private static final String amiId = "ami-0742b4e673072066f"; //TODO: CHECK IF GOOD AMI- NEED TO INSTALL SOMETHING ELSE?


    //TODO: generailize scripts?
    private static final String managerScript =
        "#cloud-boothook\n"+
        "#!/bin/bash\n"+
        "aws s3 cp s3://assignment1-pre-uploaded-jar/sarcasm.jar sarcasm.jar\n"+
        "java -jar sarcasm.jar Manager";

    private static final String workerScript =
            "#cloud-boothook\n"+
            "#!/bin/bash\n"+
            "aws s3 cp s3://assignment1-pre-uploaded-jar/sarcasm.jar sarcasm.jar\n"+
            "java -jar sarcasm.jar Worker";

    //EC2
    public static void runManager() {
        boolean isManager = false;
        // get instances
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                System.out.println(id);
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
            createEC2Instance(amiId, managerScript, Defs.MANAGER_TAG);
        }
    }

    public static void createWorkerInstance() {
        createEC2Instance(amiId, workerScript, Defs.WORKER_TAG);
    }

    public static void createWorkerInstances(int n) {
        for (int i = 0; i < n; i++) {
            createWorkerInstance();
        }
    }

    private static void createEC2Instance(String ami, String script, Tag tag) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
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

    public static int activeWorkers() {
        int activeWorkers = 0;
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                List<Tag> tags = instance.tags();
                for (Tag tag : tags) {
                    if (tag.equals(Defs.WORKER_TAG) && instance.state().name() == InstanceStateName.RUNNING) {
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
                    if (tag.equals(Defs.WORKER_TAG) && instance.state().name() == InstanceStateName.RUNNING) {
                        ids.add(id);
                    }
                }
            }
        }
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(ids)
                .build();
        ec2.terminateInstances(request);
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

}
