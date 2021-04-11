import java.io.InputStream;
import java.util.*;
import java.util.UUID;
import software.amazon.awssdk.services.sqs.model.*;

public class LocalApplication {

    public static void main(String[] args) {
        final String localApplicationID = UUID.randomUUID().toString();

        //check if manager is active on EC2 cloud, if not- start the manager node.
        AWSHelper.runManager();
        AWSHelper.initQueues();

        // S3 folder for uploading input files
        String bucket = AWSHelper.createBucket(localApplicationID);

        int N = 5;//TODO: args.length % 2 == 0 ? (args.length - 2) / 2 : (args.length - 1) / 2;
        int n = 3;
        boolean terminate = false;

        for (int i = 0; i<N; i++) {
            String key = args[i];
            AWSHelper.uploadToS3(bucket, key, key);
            //TODO: decide which message to send
            String s3URL = "s3://"+bucket+"/"+key;
            String objectURL = "https://"+bucket+".s3.amazonaws.com/" + key;
            String body = localApplicationID + ":" + bucket + ":" + key + ":" + n + ":" + terminate;
            AWSHelper.sendMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, body);
        }

// **********************************************************

        String summaryMsg = null;
        while (summaryMsg == null) {
            // receive messages from the queue
            List<Message> responseMessages = AWSHelper.receiveMessages(Defs.MANAGER_RESPONSE_QUEUE_NAME);
            for (Message m : responseMessages) {
                // msg = <localApplicationID>:<summaryURL>
                String[] content = m.body().split(":");
                if (content[0].equals(localApplicationID)) {
                    summaryMsg = content[1];
                    AWSHelper.deleteMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, m);
                    break;
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        //summaryMsg - <>:<>:<>
//        String key = summaryMsg;
//        InputStream summaryStream = AWSHelper.downloadFromS3(bucket, key);
        //TODO:  create HTML file from summary stream
    }
}
