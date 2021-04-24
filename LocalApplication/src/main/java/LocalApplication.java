import java.util.*;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.sqs.model.*;

public class LocalApplication {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        final String localApplicationID = UUID.randomUUID().toString();
        final String in = Defs.internalDelimiter;
        final String ex = Defs.externalDelimiter;
        //check if manager is active on EC2 cloud, if not- start the manager node.
        AWSHelper.runManager();
        AWSHelper.initQueues();

        // S3 folder for uploading input files
        String bucket = AWSHelper.createBucket(localApplicationID);
        List<String> keys = new ArrayList<>();
        // based on legal input
        int N = (args.length - 1) / 2;
        String n = args[2 * N];
        boolean terminate = args.length % 2 == 0;

        for (int i = 0; i < N; i++) {
            String key = args[i];
            keys.add(key);
            AWSHelper.uploadFileTOS3(bucket, key, key);
            // request - <localApplicationID><inputNum><bucket><key><n>
            AWSHelper.initQueue(Defs.WORKER_RESPONSE_QUEUE_NAME+localApplicationID+i);
            String request = localApplicationID + in + i + in + bucket + in + key + in + n;
            AWSHelper.sendMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, request);
            System.out.println("local uploaded & sent file " + key);
        }

        for (int i = 0; i < N; i++) {
            boolean relevantResponse = false;
            System.out.println("response waiting: "+i);
            while (!relevantResponse) {
                // receive messages from the queue
                List<Message> responseMessages = AWSHelper.receiveMessages(Defs.MANAGER_RESPONSE_QUEUE_NAME);
                for (Message msg : responseMessages) {
                    // response - <localApplicationID><inputNum><bucket><key>
                    String[] content = msg.body().split(in);
                    String receivedID = content[0];
                    Integer inputNum = Integer.parseInt(content[1]);
                    String receivedBucket = content[2];
                    String receivedKey = content[3];
                    if (localApplicationID.equals(receivedID)) {
                        keys.add(receivedKey);
                        executor.execute(new LocalApplicationTask(args[N + inputNum], receivedBucket, receivedKey));
                        System.out.println("local received response from manager " + msg.body());
                        AWSHelper.deleteMessage(Defs.MANAGER_RESPONSE_QUEUE_NAME, msg);
                        relevantResponse = true;
                        break;
                    }
                }
            }
        }

        System.out.printf("wait for other threads to finish");
        if (terminate) {
            AWSHelper.sendMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, Defs.TERMINATE_MESSAGE);
        }
        executor.shutdown();

        // wait for threads to finish
        while (true) {
            try {
                if (executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    break;
                }
                System.out.println("whiling");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        AWSHelper.deleteS3Bucket(bucket, keys);
        for (int i = 0; i < N; i++) {
            AWSHelper.deleteQueue(Defs.WORKER_RESPONSE_QUEUE_NAME + localApplicationID + i);
        }
        System.out.println("total time for 2 input files with " + n + "= " + n + ": " + (System.currentTimeMillis() - startTime));
    }
}
