import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

public class LocalApplication {

    public static void main(String[] args) {

        System.out.println("trying to create instances");
        //check if manager is active on EC2 cloud
        // if not- start the manager node.

        String type = "Manager";
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

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
//                .userData(Base64.getEncoder().encodeToString(user_data_string.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Type")
                .value(type)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf( "Successfully started EC2 instance %s based on AMI %s\n", instanceId, amiId);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Done!");


        int N = args.length % 2 == 0 ? (args.length - 2) / 2 : (args.length - 1) / 2;

        for (int i = 0; i<N;i++) {
            // upload file_i;
            //get location of file_i on S3
            //send location to SQS
        }

    }



}
