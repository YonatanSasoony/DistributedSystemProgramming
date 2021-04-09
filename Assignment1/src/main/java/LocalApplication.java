import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
public class LocalApplication {

    public static void main(String[] args) {

        final Tag MANAGER_TAG = Tag.builder()
                .key("Type")
                .value("Manager")
                .build();
        final Tag WORKER_TAG = Tag.builder()
                .key("Type")
                .value("Worker")
                .build();
        System.out.println("trying to create instances");
        //check if manager is active on EC2 cloud
        // if not- start the manager node.

        String amiId = "ami-0742b4e673072066f";

//        AwsSessionCredentials c = AwsSessionCredentials.create()

        String user_data_string = "#!/bin/bash\n" +
                "set -e -x\n" +
                "export DEBIAN_FRONTEND=noninteractive\n" +
                "sudo apt-get update && apt-get upgrade -y\n" +
                "sudo apt-get install wget\n" +
                "sudo apt-get install java1.8\n"+
                "sudo /usr/sbin/alternatives --config java\n"+
                "sudo /usr/sbin/alternatives --config javac\n"+
                "echo \"Please remember to set the MySQL root password!\"";

        // snippet-start:[ec2.java2.create_instance.main]
        Ec2Client ec2 = Ec2Client.create();

        boolean isManager = false;
        // get instances
        for (Reservation reservation : ec2.describeInstances().reservations()) {
            for (Instance instance : reservation.instances()) {
                String id = instance.instanceId();
                System.out.println(id);
                List<Tag> tags = instance.tags();
                for (Tag tag : tags) {
                    if (tag.equals(MANAGER_TAG) && instance.state().name() != InstanceStateName.TERMINATED) {
                        System.out.println("manager found, state: "+instance.state().name() +" id:" + id);
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
            // creating new instance
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
//                .userData(Base64.getEncoder().encodeToString(user_data_string.getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();

            // creating a manager tag for instance

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(MANAGER_TAG)
                    .build();
            try {
                ec2.createTags(tagRequest);
                System.out.printf( "Successfully started EC2 instance %s based on AMI %s\n", instanceId, amiId);

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }

        // manager is running
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Done!");

        System.out.println("Trying to create SQS");
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        final String QUEUE_NAME = "testQueue" + new Date().getTime();
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;
        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Done creating SQS");


        int N = 5;//TODO: args.length % 2 == 0 ? (args.length - 2) / 2 : (args.length - 1) / 2;

        System.out.println("Trying to upload file to S3.");

        Region region = Region.US_EAST_1;

        S3Client s3 = S3Client.builder().region(region).build();
        String bucket = "bucket" + System.currentTimeMillis();

        s3.createBucket(CreateBucketRequest
            .builder()
            .bucket(bucket)
            .build());

        try {
            for (int i = 0; i<N; i++) {
                String key = args[i];
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                        RequestBody.fromFile(new File(key)));
                //TODO: decide which message to send
                String s3URL = "s3://"+bucket+"/"+key;
                String objectURL = "https://"+bucket+".s3.amazonaws.com/"+key;
                String body = bucket+":"+key;
                SendMessageRequest send_msg_request = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(body)
//                        .delaySeconds(5) // TODO: check this line
                        .build();
                sqs.sendMessage(send_msg_request);

            }
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("Upload to S3 failed.");
        }


        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        List<Book> books = new ArrayList<>();
        System.out.println("Reading messages");
        for (Message m : messages) {
            // TODO: parse message to Book instance
            String body = m.body();
            System.out.println(body);
            String[] content = body.split(":");
            String b = content[0];
            String k = content[1];
//            ResponseBytes<GetObjectResponse> response = s3.getObject(GetObjectRequest.builder().bucket(b).key(k).build(),
//                    ResponseTransformer.toBytes());
            ResponseInputStream<GetObjectResponse> response = s3.getObject(GetObjectRequest.builder().bucket(b).key(k).build());



//            System.out.println(new String(response.asByteArray()));
            System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@22");


            books.add(JSONBookParser.parse(response));
        }
        for (Book book : books) {
            System.out.println(book);
            System.out.println("##############################################################################");
        }
        // TODO: stop manager? delete buckets?
    }
}
