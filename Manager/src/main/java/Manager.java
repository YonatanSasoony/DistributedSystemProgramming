import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) {
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        boolean isTerminated = false;
        while (!isTerminated) {
            // receive messages from the queue
            List<Message> requestMessages = AWSHelper.receiveMessages(Defs.MANAGER_REQUEST_QUEUE_NAME);
            System.out.println("request messages size: "+requestMessages.size());
            for (Message msg : requestMessages) {
                // request - <localApplicationID><inputNum><bucket><key><n><terminate>
                System.out.println("manager received message: "+msg.body());
                String[] content = msg.body().split(Defs.internalDelimiter);
                String localAppId = content[0];
                String inputNum = content[1];
                String bucket = content[2];
                String key = content[3];
                Integer n = Integer.parseInt(content[4]);
                Boolean terminate = Boolean.parseBoolean(content[5]);

                Product product = JSONProductParser.parse(AWSHelper.downloadFromS3(bucket, key));
                System.out.println(product.title());
                int reviewsNum = product.reviews().size();
                int requiredWorkers = (int)(Math.ceil(reviewsNum / n)); // m
                int activeWorkers = AWSHelper.activeWorkers();
                int numOfWorkersToAdd = requiredWorkers - activeWorkers;
//                AWSHelper.createWorkerInstances(numOfWorkersToAdd); //TODO:
                executor.execute(new ManagerTask(localAppId, inputNum, product, bucket));
                AWSHelper.deleteMessage(Defs.MANAGER_REQUEST_QUEUE_NAME, msg);
                if (terminate) {
                    executor.shutdown();
                    isTerminated = true;
                }
            }
        }

        // wait for threads to finish
        while (true) {
            try {
                if (executor.awaitTermination(2, TimeUnit.SECONDS)) break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        AWSHelper.shutdownWorkers();
    }
}
